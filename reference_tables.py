# Databricks notebook source
dbutils.widgets.text('patch_or_patch_type', '', 'specify patch or patch_type, else most recent')
dbutils.widgets.text('catalog', '', 'Catalog to write/read')
dbutils.widgets.text('database', 'tft_reference', 'Database to write to')
dbutils.widgets.text('override_set_mutator', '', 'Custom set mutator, highest standard by default')
dbutils.widgets.text('patch_override', '', 'Forced patch number for when there is an issue with Cdragon patch')

# COMMAND ----------

import requests
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import re
from pyspark.sql.types import StringType, ArrayType, IntegerType, StructField, StructType, BooleanType, MapType
from utils.update_references import update_latest_uc
from utils.etl.league_patch import get_patch
from utils.df_utils import keep_first_rows_per_window

# COMMAND ----------

# DBTITLE 1,Util functions
def save_patch_reference(df, table_name, catalog):
    (
        df
        .write
        .format('delta')
        .mode('overwrite')
        .partitionBy('patch', 'set_mutator')
        .option('partitionOverwriteMode', 'dynamic')
        .saveAsTable(f"{catalog}.tft_reference.{table_name}")
    )


def format_image_url(icon):
    """
    Used to have a image url for given entities given icon field
    """
    global patch
    if icon:
        return f"https://raw.communitydragon.org/{patch}/game/{icon.lower().replace('.tex', '.png').replace('.dds', '.png')}"
    return icon
    
format_image_url_udf = F.udf(format_image_url, StringType())

stringify_col = lambda col: F.col(col).cast('string').alias(f'_{col}_str')

# COMMAND ----------

patch_or_patch_type_override = dbutils.widgets.get('patch_or_patch_type')
catalog = dbutils.widgets.get('catalog')
database = "tft_reference"
override_set_mutator = dbutils.widgets.get('override_set_mutator') or None

assert catalog

patch_or_patch_type = patch_or_patch_type_override or 'latest'
patch = get_patch(patch_or_patch_type)
patch = dbutils.widgets.get('patch_override') or patch

# Conditions to update `latest` and `pbe`tables
update_latest = (patch_or_patch_type == 'latest') & (override_set_mutator is None)
update_pbe = (patch_or_patch_type == 'pbe') & (override_set_mutator is None)

print(f"patch_or_patch_type: {patch_or_patch_type}")
print(f"patch: {patch}")
print(f"catalog: {catalog}")
print(f"database: {database}")
print(f"override_set_mutator: {override_set_mutator}")


# COMMAND ----------

trait_counts_schema = MapType(StringType(), IntegerType())

# COMMAND ----------

# MAGIC %md
# MAGIC # Get Data

# COMMAND ----------

TFT_PATCH_DATA_URL = 'https://raw.communitydragon.org/{patch}/cdragon/tft/en_us.json'
url = TFT_PATCH_DATA_URL.format(patch=patch_or_patch_type)
url

# COMMAND ----------

r = requests.get(url)

if r.status_code != 200:
    raise ValueError('Failed to request data for patch: %s' % patch)

data = r.json()

# COMMAND ----------

# MAGIC %md
# MAGIC # SetData
# MAGIC
# MAGIC Useful to keep track which patch belongs to which set in case we need to make set specific treatment

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set

# COMMAND ----------

all_sets_data = data['setData']

if not override_set_mutator:
    # Standard mutators (standard modes) regex
    # Evolved is from Set13, it's the equivalent of `Stage2`.
    standard_mutator_regex_pattern = r'^TFTSet\d+(_(?:Stage2|Evolved))?$'

    # Filter standard cdragon set entities
    standard_set_data = [
        set_data for set_data in all_sets_data
        if re.match(standard_mutator_regex_pattern, set_data['mutator'])
    ]

    # Higher number or highest mutator (string) => TFTSetX_Stage2 > TFTSetX
    set_data = max(standard_set_data, key=lambda x: (x['number'], x['mutator']))

else:
    # Iterate through sets to find target one
    found_set = False
    for set_data in all_sets_data:
        if set_data['mutator'] == override_set_mutator:
            found_set = True
            break

    # Not found
    if not found_set:
        raise ValueError(f"Override set_mutator {override_set_mutator} not found")

set_name = set_data['name']
set_mutator = set_data['mutator']
season_number = str(set_data['number'])

# Mid set information is contained in mutator suffix
set_number = season_number + '.5' if set_mutator.endswith('_Stage2') else season_number

set_metadata = {
    'set_name': set_name,
    'set_number': set_number,
    'set_mutator': set_mutator,
    'season_number': season_number,
    'is_default': True if not override_set_mutator else False
}

print("Found set", set_metadata)

sets_schema = StructType([
    StructField('set_name', StringType(), True),
    StructField('set_number', StringType(), True),
    StructField('set_mutator', StringType(), True),
    StructField('season_number', StringType(), True),
    StructField('is_default', BooleanType(), True),
])

sets_df = spark.createDataFrame([set_metadata], schema=sets_schema).withColumn('patch', F.lit(patch))

# COMMAND ----------

save_patch_reference(sets_df, 'sets', catalog=catalog)

# Update latest (overwrite and notify through slack if any modifications)
if update_latest:
    update_latest_uc(sets_df, 'sets_latest', schema=database, catalog=catalog, ignore_cols=[])

# Update pbe (for tracking purposes only)
if update_pbe:
    update_latest_uc(sets_df, 'sets_pbe', schema=database, catalog=catalog, ignore_cols=[])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Traits

# COMMAND ----------

traits = []

# Used to map trait in other objects
trait_name_api_name_mapping = {}

for trait_data in set_data['traits']:
    trait = {
        'api_name': trait_data['apiName'],
        'icon': trait_data['icon'],
        'effects': trait_data['effects'],
        'name': trait_data['name'],
        'set_mutator': set_mutator,
        'needed_unit_counts': [effect['minUnits'] for effect in trait_data['effects']],
    }

    trait_name_api_name_mapping[trait['name']] = trait['api_name']

    traits.append(trait)

# COMMAND ----------

trait_schema = StructType([
    StructField('api_name', StringType(), True),
    StructField('effects', ArrayType(MapType(StringType(), StringType())), True),
    StructField('icon', StringType(), True),
    StructField('name', StringType(), True),
    StructField('set_mutator', StringType(), True),
    StructField('needed_unit_counts', ArrayType(IntegerType()), True)
])

traits_df = (
    spark.createDataFrame(traits, trait_schema)

    .withColumn('patch', F.lit(patch))

    # Adding string versions of complex structs for evol tracking
    .select(
        '*',
        stringify_col('effects')
    )
)

# COMMAND ----------

save_patch_reference(traits_df, 'traits', catalog=catalog)

# Update latest (overwrite and notify through slack if any modifications)
if update_latest:
    # Ignoring stats column as MapType() columns can't be transformed through 'except' transformation
    update_latest_uc(traits_df, 'traits_latest', schema=database, catalog=catalog, ignore_cols=['patch', 'effects'])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Champions

# COMMAND ----------

from src.tft.reference_hardcoded import champion_lcu_cdragon_mapping, generic_unit_mapping

# COMMAND ----------

# DBTITLE 1,LCU data - Cdragon mapping
if set_mutator not in champion_lcu_cdragon_mapping:
    assert ('No Champion LCU/CDragon mapping found for set %s' % set_mutator)

cdragon_api_name_lcu_icon_mapping = {
    cdragon_api_name: lcu_icon
    for lcu_icon, cdragon_api_name in
    champion_lcu_cdragon_mapping[set_mutator]
}

# COMMAND ----------

champions = []

for champion_data in set_data['champions']:
    champion = {
        'api_name': champion_data['apiName'],
        'api_name_generic': generic_unit_mapping.get(set_mutator, {}).get(champion_data['apiName'], champion_data['apiName']),
        'cost': champion_data['cost'],
        'icon': champion_data['icon'],
        'name': champion_data['name'],
        'stats': champion_data['stats'],
        'traits': champion_data['traits'],
        'set_mutator': set_mutator,
        'image_url': format_image_url(champion_data['tileIcon']),
    }

    # Sometimes cost is above 5, if that's the case we set it to 0 as it's always special unit
    if champion['cost'] > 6:
        print(champion)
        champion['cost'] = 0

    # Adding LCU icon for joining with LCU data
    champion['lcu_icon'] = cdragon_api_name_lcu_icon_mapping.get(champion['api_name'])
    if not champion['lcu_icon']:
        print('Missing mapping for %s' % champion['name'])

    # Creating trait counts
    champion['trait_counts'] = {
        trait_name_api_name_mapping[trait]: 1
        for trait in champion['traits']
    }

    # Default board size
    champion['board_size'] = 1

    # More champions in referential than in the set of playable champions
    # Champions without any trait are not playable champions
    # TODO: Technique isn't perfect, find another way
    champion['is_playable'] = len(champion['traits']) > 0

    champions.append(champion)

# COMMAND ----------

champion_schema = StructType([
    StructField('api_name', StringType(), True),
    StructField('api_name_generic', StringType(), True),
    StructField('cost', IntegerType(), True),
    StructField('icon', StringType(), True),
    StructField('name', StringType(), True),
    StructField('stats', MapType(StringType(), StringType()), True),
    StructField('traits', ArrayType(StringType()), True),
    StructField('trait_counts', MapType(StringType(), IntegerType()), True),
    StructField('set_mutator', StringType(), True),
    StructField('lcu_icon', StringType(), True),
    StructField('image_url', StringType(), True),
    StructField('board_size', IntegerType(), True),
    StructField('is_playable', BooleanType(), True),
])

champions_df = (
    spark.createDataFrame(champions, champion_schema)
    .withColumn('patch', F.lit(patch))
)

# COMMAND ----------

# DBTITLE 1,Overwrite fields for some specific champions
from src.tft.reference_hardcoded import champion_fields_overwrite

champion_fields_overwrite_df = spark.createDataFrame(champion_fields_overwrite, champion_schema)

# Columns to overwrite
overwrite_columns = [column for column in champion_fields_overwrite_df.columns if column != 'api_name']

# Join with df
champions_df = (
    champions_df
    .join(
        champion_fields_overwrite_df.select(
            'api_name',
            *[
                F.col(column).alias(column + '_overwrite')
                for column in overwrite_columns
            ]
        ),
        on='api_name',
        how='left'
    )

    # Adding string versions of complex structs for evol tracking
    .select(
        '*',
        stringify_col('trait_counts'),
        stringify_col('stats')
    )
)

# Overwrite columns
for column in overwrite_columns:
    champions_df = (
        champions_df
        .withColumn(
            column,
            F.coalesce(F.col(column + '_overwrite'), F.col(column))
        )
    )

# Drop temporary columns
champions_df = champions_df.drop(*[column + '_overwrite' for column in overwrite_columns])

# COMMAND ----------

save_patch_reference(champions_df, 'champions', catalog=catalog)

# Update latest (overwrite and notify through slack if any modifications)
if update_latest:
    # Ignoring stats, trait_counts columns as MapType() columns can't be transformed through 'except' transformation
    update_latest_uc(champions_df, 'champions_latest', schema=database, catalog=catalog, ignore_cols=['patch', 'stats', 'trait_counts'])

# COMMAND ----------

# MAGIC %md
# MAGIC # Items
# MAGIC
# MAGIC In cdragon, what they call `items` actually contains champion items, components and other consommables  

# COMMAND ----------

item_schema = StructType([
    StructField('apiName', StringType(), True),
    StructField('desc', StringType(), True),
    StructField('effects', MapType(StringType(), StringType()), True),
    StructField('from', ArrayType(StringType()), True),
    StructField('composition', ArrayType(StringType()), True),
    StructField('icon', StringType(), True),
    StructField('id', StringType(), True),
    StructField('name', StringType(), True),
    StructField('unique', BooleanType(), True),
])

items_df = (
    spark.createDataFrame(data['items'], item_schema)
    .withColumn('patch', F.lit(patch))
    .withColumn('set_mutator', F.lit(set_mutator))
)

items_df = (
    items_df

    # Infer category based on apiName
    .withColumn(
        'category',
        F.split('apiName', '_').getItem(1)
    )

    .withColumn(
        'image_url',
        format_image_url_udf('icon')
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Augments
# MAGIC
# MAGIC Augments are `items` with **Augment** category
# MAGIC
# MAGIC :warning: There is more augments in cdragon than in the set. Cdragon does not remove ALL old augments.
# MAGIC It is okay for doing statistics, as when agregating only augments with data will come out. It is not for FE in case we want an exhaustive list

# COMMAND ----------

# DBTITLE 1,Extracting bonus trait of trait augments
COUNT_REGEX = r"^.+having (?P<nb>[0-9]) additional.+$"

# Everything before the last word
TRAIT_REGEX = r"^(?P<trait>.+) [a-zA-Z]+$"

def extract_augment_trait_counts(name, desc):
    """
    desc examples: 
    - Your team counts as having 1 additional Evoker. Gain a Zyra.
    """
    if not desc:
        return None

    count_match = re.match(COUNT_REGEX, desc)
    trait_match = re.match(TRAIT_REGEX, name)

    # Check if it's a trait augment, must match the 2 regex
    if (not count_match) or (not trait_match):
        return None

    count = int(count_match.groupdict()['nb'])
    trait = trait_match.groupdict()['trait']
    trait_api_name = trait_name_api_name_mapping.get(trait, None)

    if not trait_api_name:
        return None

    trait_counts = {
        trait_api_name: count
    }

    return trait_counts

# # Simple examples
# assert extract_augment_trait_counts('Shimmerscale Soul', 'Your team counts as having 1 additional Shimmerscale. Gain a Jax.') == {'Shimmerscale': 1}
# assert extract_augment_trait_counts('Swiftshot Heart', 'Your team counts has having 1 additional Swiftshot. Gain a Twitch.') == {'Swiftshot': 1}

# # Example with more than 1 additional
# assert extract_augment_trait_counts('Clockwork Soul', 'Your team counts as having 2 additional Clockworks. Gain @Gold@ gold.') == {'Clockwork': 2}

# # Example with space in trait name
# assert extract_augment_trait_counts('Star Guardian Heart', 'Your team counts as having 1 additional Star Guardian. Gain a Yuumi.') == {'Star Guardian': 1}

# # Counter example
# assert extract_augment_trait_counts('Ancient Archives II', 'Gain @NumTomes@ Tome of Traits.') == None

extract_augment_trait_counts_udf = F.udf(extract_augment_trait_counts, trait_counts_schema)

# COMMAND ----------

# DBTITLE 1,Extract rarity based on icon
ICON_RARITY_REGEX = r"^ASSETS\/Maps\/TFT\/Icons\/Augments\/Hexcore\/[^.]+((?P<arabic>[1-3])|[-_](?P<roman>[I]+)|(?<![-_I])(?P<roman_end>[I]+)).+$"


def extract_augment_rarity(icon):
    icon_match = re.match(ICON_RARITY_REGEX, icon)

    if not icon_match:
        return None

    icon_match_dict = icon_match.groupdict()

    arabic_digit = icon_match_dict.get('arabic')
    roman_digit = icon_match_dict.get('roman')
    roman_end_digit = icon_match_dict.get('roman_end')

    if roman_digit:
        rarity = len(roman_digit)

    if roman_end_digit:
        rarity = len(roman_end_digit)

    elif arabic_digit:
        rarity = int(arabic_digit)

    return rarity

assert extract_augment_rarity('ASSETS/Maps/TFT/Icons/Augments/Hexcore/Freljord-Heart-I.TFT_Set9.tex') == 1 # roman hyphen before
assert extract_augment_rarity('ASSETS/Maps/TFT/Icons/Augments/Hexcore/Stars-are-born-II.TFT_Set9.tex') == 2 # roman hyphen before
assert extract_augment_rarity('ASSETS/Maps/TFT/Icons/Augments/Hexcore/Pirates2.tex') == 2  # arabic
assert extract_augment_rarity('ASSETS/Maps/TFT/Icons/Augments/Hexcore/Tiniest-TitanIII.tex') == 3 # roman no hyphen
assert extract_augment_rarity('ASSETS/Maps/TFT/Icons/Augments/Hexcore/Battlemage-III-A.tex') == 3 # roman hypen before after

extract_augment_rarity_udf = F.udf(extract_augment_rarity, IntegerType())

# COMMAND ----------

# A dict of icon names that do not fit our regex pattern and need to be overwritten
bad_icon_names = {
    'ASSETS/Maps/TFT/Icons/Augments/Hexcore/Tiniest-TitanIII.TFT_Set9.tex': 3,
    'ASSETS/Maps/TFT/Icons/Augments/Hexcore/TFT_Augment_EagleEye.tex': 2,
    'ASSETS/Maps/TFT/Icons/Augments/Hexcore/NoScope.tex': 2,

    # Set14
    'ASSETS/Maps/Particles/TFT/Item_Icons/Traits/Spatula/Set14/TFT14_Emblem_StreetDemon.TFT_Set14.tex': 2,
    'ASSETS/Maps/TFT/Icons/TFT14/TFT14_Template_Augment_Icon.TFT_Set14.tex': 1 # Template, fake value
}

# COMMAND ----------

augments_df = (
    items_df
    .where(F.col('category') == 'Augment')
    .drop('category', 'composition')

    # Extract trait counts for trait augments
    .withColumn(
        'trait_counts',
        extract_augment_trait_counts_udf('name', 'desc')
    )

        # Extract trait counts for trait augments
    .withColumn(
        'rarity',
        F.when(
            F.col('icon').isin(list(bad_icon_names.keys())),
            F.udf(lambda icon: bad_icon_names.get(icon, None), IntegerType())('icon')
        )
        .otherwise(
            extract_augment_rarity_udf('icon')
        )
    )
)

# Making sure all augments have an augmnent
no_rarity_augments = augments_df.where(F.col('rarity').isNull())
if no_rarity_augments.count() != 0:
    display(
        no_rarity_augments
    )
    raise ValueError("Some augments have no rarity")

# COMMAND ----------

# DBTITLE 1,Legend augments handling, through hardcoded mapping
from src.tft.reference_hardcoded import legend_augments

legent_augments_tier_schema = StructType([
    StructField('stage1', StringType(), True),
    StructField('stage2', StringType(), True),
    StructField('stage3', StringType(), True),
])

legend_augments_schema = StructType([
    StructField('legend_id', StringType(), True),
    StructField('1', legent_augments_tier_schema, True),
    StructField('2', legent_augments_tier_schema, True),
    StructField('3', legent_augments_tier_schema, True),
])

# Load raw data
legend_augments_list = [
    {
        'legend_id': legend_id,
        **legend_augments.get(set_mutator)[legend_id]
    }
    for legend_id in legend_augments.get(set_mutator, {}).keys()
]
raw_legend_augments_df = spark.createDataFrame(
    legend_augments_list,
    schema=legend_augments_schema
)

# Function to process columns
def format_tier_legend_augments(tier):
    return [
        F.struct(
            F.lit(tier).cast('string').alias('tier'),
            F.lit(f'stage{stage}').alias('stage'),
            F.col(f'{tier}.stage{stage}').alias('apiName')
        )
        for stage in [1, 2, 3]
    ]

# Format each augment as one row
legend_augments_df = (
    raw_legend_augments_df
    .select(
        'legend_id',
        F.explode(F.array(
            format_tier_legend_augments(1) +
            format_tier_legend_augments(2) +
            format_tier_legend_augments(3)
        )).alias('augment')
    )

    .select(
        'legend_id',
        'augment.*'
    )
)

# Make it one row per augment
legend_augments_df = (
    legend_augments_df
    .where(F.col('apiName').isNotNull())
    .groupBy(
        'apiName'
    )
    .agg(
        # Get all legends, sort them to make sure later comparison always works
        F.sort_array(F.collect_set('legend_id')).alias('legend_ids'),

        # We assume tier and stage to be the same for the same augment but diff legends
        F.first('tier').alias('tier'),
        F.first('stage').alias('stage'),
    )
)

# Add to augments referential
augments_df = (
    augments_df
    .join(
        legend_augments_df,
        on='apiName',
        how='left'
    )

    # Adding string versions of complex structs for evol tracking
    .select(
        '*',
        stringify_col('trait_counts'),
        stringify_col('effects'),
        stringify_col('legend_ids'),
    )
)

# COMMAND ----------

save_patch_reference(augments_df, 'augments', catalog=catalog)

# Update latest (overwrite and notify through slack if any modifications)
if update_latest:
    # Ignoring effect column as MapType() columns can't be transformed through 'except' transformation
    update_latest_uc(augments_df, 'augments_latest', schema=database, catalog=catalog, ignore_cols=['patch', 'effects', 'trait_counts', 'legend_ids'])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unit items
# MAGIC
# MAGIC :warning: Same than items, cdragon contains more than there are
# MAGIC

# COMMAND ----------

# DBTITLE 1,Extracting bonus trait of emblems
# Note: Would previously assign emblem trait incorrectly if a trait is a subset of another (i.e. Guardian and StarGuardian) so the keys must be sorted by length in trait_names and the regex
# Example: If StarGuardian is evaluated first, it won't be mistaken for Guardian which is a subset. Guardian cannot be evaluated as StarGuardian because it is a shorter word and cannot be a subset.

# Using previously parsed traits to extract exhaustive list of emblems we can encounter
    # Map trait api_name suffix to trait name

trait_api_name_suffix_to_name_mapping = {
    x['api_name'].split('_')[1] if '_' in x['api_name'] else x['api_name']: x['name']
    for x in traits_df.collect()
}

# Get exhaustive list of trait names
trait_names = sorted([x['name'] for x in traits_df.collect()], key=len, reverse=True)

def gen_extract_item_trait_counts(trait_api_name_suffix_to_name_mapping):
    """
    Generator. Returns a function
    """
    # Dynamically create regex based on list of available traits
    sorted_trait_api_name_sufix_to_name_mapping_keys = sorted(trait_api_name_suffix_to_name_mapping.keys(), key=len, reverse=True)
    trait_regex = f"^.+_(?P<trait_api_name_suffix>{'|'.join(sorted_trait_api_name_sufix_to_name_mapping_keys)})EmblemItem$"

    def extract_item_trait_counts(api_name, name):
        """
        Extract trait_counts given by an item given its api_name or its name
        Having a single method is not enough to succesfully extract all traits (as of set 8)
        For this reason 2 methods are used, in order to guarantee a trait match.
        """
        if (not name) or (not api_name):
            return None

        # 1st method: api_name matching (from trait to item)
        emblem_match = re.match(trait_regex, api_name)
        if emblem_match:
            # Extract the trait's api_name suffix, contained in item's api_name
            trait_api_name_suffix = emblem_match.groupdict()['trait_api_name_suffix']
            trait_name = trait_api_name_suffix_to_name_mapping[trait_api_name_suffix]

        # 2nd method: check if any trait name is included in item name
        else:
            found_trait = False
            for trait_name in trait_names:
                if trait_name in name:
                    found_trait = True
                    break

            if not found_trait:
                return None

        trait_api_name = trait_name_api_name_mapping[trait_name]

        # Count always 1
        trait_counts = {
            trait_api_name: 1
        }

        return trait_counts

    return extract_item_trait_counts

test_extract_item_trait_counts = gen_extract_item_trait_counts(trait_api_name_suffix_to_name_mapping)
# assert test_extract_item_trait_counts('TFT8_Item_GenAEEmblemItem', 'Gadgeteen Emblem') == {'Gadgeteen': 1}
# assert test_extract_item_trait_counts('TFT8_Item_ExoPrimeEmblemItem', 'Mecha: PRIME Emblem') == {'Mecha:PRIME': 1}
# assert test_extract_item_trait_counts('TFT8_Item_InterPolarisEmblemItem', 'LaserCorps Emblem') == {'LaserCorps': 1}
# assert test_extract_item_trait_counts('TFT8_Item_ADMINEmblemItem', 'A.D.M.I.N. Emblem') == {'A.D.M.I.N.': 1}

extract_item_trait_counts_udf = F.udf(gen_extract_item_trait_counts(trait_api_name_suffix_to_name_mapping), trait_counts_schema)

# COMMAND ----------

# Items we care about are only a subset of general items, set-specific item categories are considered as 'special'.
special_items = ['HeimerUpgrade']
sub_items_df = (
    items_df
    .where(F.col('category').isin(['Item', 'Consumable', 'Assist'] + special_items))
    .drop('category')
)

# Collect exhaustive list of base components by getting all craftable items base components
base_component_api_names = [
    row['item_api_name']
    for row in
    (
        sub_items_df
        .select(
            F.explode('composition').alias('item_api_name')
        )
        .distinct()
        .collect()
    )
]

def get_item_type(item_api_name, item_id, item_icon):
    if item_api_name in base_component_api_names:
        return 'base_component'

    if 'Emblem' in item_api_name:
        return 'emblem'

    if item_icon.startswith('ASSETS/Maps/Particles/TFT/Item_Icons/Ornn_Items/'):
        return 'ornn'

    if item_icon.startswith('ASSETS/Maps/Particles/TFT/Item_Icons/Radiant/'):
        return 'radiant'

    if item_icon.startswith('ASSETS/Maps/Particles/TFT/Item_Icons/TFT9_SupportItems/'):
        return 'support'

    return 'other'

get_item_type_udf = F.udf(get_item_type, StringType())


# Failsafe to override incorrect data
non_craftable_item_types = ['radiant', 'support', 'ornn', 'base_component']
sub_items_df = (
    sub_items_df

    # Infer item type 
    .withColumn(
        'type',
        get_item_type_udf('apiName', 'id', 'icon')
    )

    # Infer craftability
    .withColumn(
        'is_craftable',
        F.when(
            F.col('type').isin(non_craftable_item_types),
            False
        )
        .otherwise(
            F.size('composition') > 0
        )
    )

    # Enforce noncraftability
    .withColumn(
        'composition',
        F.when(
            F.col('is_craftable'),
            F.col('composition')
        )
        .otherwise(
            F.array()
        )
    )

    # Extract trait_counts in case item is an emblem, None otherwise
    .withColumn(
        'trait_counts',
        F.when(
            F.col('type') == 'emblem',
            extract_item_trait_counts_udf('apiName', 'name')
        )
    )

    # Extract trait if it's emblem (first and unique key of trait_counts)
    .withColumn(
        'emblem_trait',
        F.map_keys('trait_counts')[0]
    )

    # Adding string versions of complex structs for evol tracking
    .select(
        '*',
        stringify_col('trait_counts'),
        stringify_col('effects')
    )
)

# COMMAND ----------

save_patch_reference(sub_items_df, 'items', catalog=catalog)

# Update latest (overwrite and notify through slack if any modifications)
if update_latest:
    # Ignoring effects column as MapType() columns can't be transformed through 'except' transformation
    update_latest_uc(sub_items_df, 'items_latest', schema=database, catalog=catalog, ignore_cols=['patch', 'effects', 'trait_counts'])


# COMMAND ----------


