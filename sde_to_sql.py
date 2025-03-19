import os
import sys
import yaml
import re
import math
from collections import defaultdict
from datetime import datetime

# Maximum number of rows per SQL file
MAX_ROWS_PER_FILE = 1000

def ensure_directory(directory):
    """Ensure the directory exists, create if it doesn't."""
    if not os.path.exists(directory):
        os.makedirs(directory)

def sanitize_string(s):
    """Sanitize a string for SQL insertion."""
    if s is None:
        return "NULL"
    s = str(s).replace("'", "''")
    return f"'{s}'"

def sanitize_value(value):
    """Sanitize a value for SQL insertion based on its type."""
    if value is None:
        return "NULL"
    elif isinstance(value, (int, float)):
        return str(value)
    elif isinstance(value, bool):
        return "1" if value else "0"
    elif isinstance(value, datetime):
        return f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'"
    else:
        return sanitize_string(str(value))

def process_universe_data(sde_dir, output_dir):
    """Process universe data (regions, constellations, solar systems)."""
    universe_dir = os.path.join(sde_dir, "universe")
    if not os.path.exists(universe_dir):
        print(f"Universe directory not found: {universe_dir}")
        return

    # Data collectors
    regions = []
    constellations = []
    solar_systems = []

    # Process each space type (eve, void, wormhole, abyssal)
    for space_type in ["eve", "void", "wormhole", "abyssal"]:
        space_dir = os.path.join(universe_dir, space_type)
        if not os.path.exists(space_dir):
            continue

        # Walk through regions
        for region_name in os.listdir(space_dir):
            region_dir = os.path.join(space_dir, region_name)
            if not os.path.isdir(region_dir):
                continue

            # Process region
            region_file = os.path.join(region_dir, "region.yaml")
            if os.path.exists(region_file):
                with open(region_file, 'r', encoding='utf-8') as f:
                    region_data = yaml.safe_load(f)
                    region_id = region_data.get('regionID')
                    region_name = region_data.get('name', {}).get('en', region_name)
                    regions.append({
                        'region_id': region_id,
                        'region_name': region_name,
                        'space_type': space_type
                    })

            # Walk through constellations
            for constellation_name in os.listdir(region_dir):
                constellation_dir = os.path.join(region_dir, constellation_name)
                if not os.path.isdir(constellation_dir) or constellation_name == "region.yaml":
                    continue

                # Process constellation
                constellation_file = os.path.join(constellation_dir, "constellation.yaml")
                if os.path.exists(constellation_file):
                    with open(constellation_file, 'r', encoding='utf-8') as f:
                        constellation_data = yaml.safe_load(f)
                        constellation_id = constellation_data.get('constellationID')
                        constellation_name = constellation_data.get('name', {}).get('en', constellation_name)
                        constellations.append({
                            'constellation_id': constellation_id,
                            'constellation_name': constellation_name,
                            'region_id': region_id,
                            'space_type': space_type
                        })

                # Walk through solar systems
                for system_name in os.listdir(constellation_dir):
                    system_dir = os.path.join(constellation_dir, system_name)
                    if not os.path.isdir(system_dir) or system_name == "constellation.yaml":
                        continue

                    # Process solar system
                    system_file = os.path.join(system_dir, "solarsystem.yaml")
                    if os.path.exists(system_file):
                        with open(system_file, 'r', encoding='utf-8') as f:
                            system_data = yaml.safe_load(f)
                            system_id = system_data.get('solarSystemID')
                            system_name = system_data.get('name', {}).get('en', system_name)
                            security = system_data.get('security', 0)
                            security_class = system_data.get('securityClass', '')
                            solar_systems.append({
                                'system_id': system_id,
                                'system_name': system_name,
                                'constellation_id': constellation_id,
                                'region_id': region_id,
                                'security_status': security,
                                'security_class': security_class,
                                'space_type': space_type
                            })

    # Create subdirectory for universe data
    universe_dir = os.path.join(output_dir, "universe")
    ensure_directory(universe_dir)
    
    # Write regions SQL
    write_sql_file(
        os.path.join(universe_dir, "eve_regions.sql"),
        "eve_solar_systems",
        ["region_id", "region_name"],
        [(r['region_id'], r['region_name']) for r in regions]
    )

    # Write constellations SQL
    write_sql_file(
        os.path.join(universe_dir, "eve_constellations.sql"),
        "eve_solar_systems",
        ["constellation_id", "constellation_name", "region_id"],
        [(c['constellation_id'], c['constellation_name'], c['region_id']) for c in constellations]
    )

    # Write solar systems SQL - might need to split by region due to size
    systems_by_region = defaultdict(list)
    for system in solar_systems:
        systems_by_region[system['region_id']].append(system)

    for region_id, systems in systems_by_region.items():
        region_name = next((r['region_name'] for r in regions if r['region_id'] == region_id), str(region_id))
        safe_name = re.sub(r'[^a-zA-Z0-9]', '_', region_name)
        
        write_sql_file(
            os.path.join(universe_dir, f"eve_solar_systems_{safe_name}.sql"),
            "eve_solar_systems",
            ["system_id", "system_name", "constellation_id", "constellation_name", "region_id", "region_name", "security_status", "security_class"],
            [
                (
                    s['system_id'], 
                    s['system_name'],
                    s['constellation_id'],
                    next((c['constellation_name'] for c in constellations if c['constellation_id'] == s['constellation_id']), None),
                    s['region_id'],
                    next((r['region_name'] for r in regions if r['region_id'] == s['region_id']), None),
                    s['security_status'],
                    s['security_class']
                ) 
                for s in systems
            ]
        )

def process_landmarks(sde_dir, output_dir):
    """Process landmarks data."""
    landmarks_file = os.path.join(sde_dir, "universe", "landmarks", "landmarks.yaml")
    if not os.path.exists(landmarks_file):
        print(f"Landmarks file not found: {landmarks_file}")
        return

    with open(landmarks_file, 'r', encoding='utf-8') as f:
        landmarks_data = yaml.safe_load(f)

    landmarks = []
    for landmark_id, data in landmarks_data.items():
        landmarks.append({
            'landmark_id': landmark_id,
            'name': data.get('name', {}).get('en', ''),
            'description': data.get('description', {}).get('en', ''),
            'location_id': data.get('locationID'),
            'x': data.get('x'),
            'y': data.get('y'),
            'z': data.get('z')
        })

    # Create subdirectory for universe data
    universe_dir = os.path.join(output_dir, "universe")
    ensure_directory(universe_dir)
    
    write_sql_file(
        os.path.join(universe_dir, "eve_landmarks.sql"),
        "eve_landmarks",
        ["landmark_id", "name", "description", "location_id", "x", "y", "z"],
        [(l['landmark_id'], l['name'], l['description'], l['location_id'], l['x'], l['y'], l['z']) for l in landmarks]
    )

def process_npc_corporations(data, columns):
    """Custom processor for NPC Corporations data."""
    rows = []
    
    for corp_id, corp_data in data.items():
        # Map the corporation data to our schema
        corporation_id = corp_id
        corporation_name = corp_data.get('nameID', {}).get('en', 'Unknown')
        ticker = corp_data.get('tickerName', '')
        ceo_character_id = corp_data.get('ceoID')
        tax_rate = corp_data.get('taxRate', 0.0)
        member_limit = corp_data.get('memberLimit', 0)
        alliance_id = None  # May not be available in SDE, would need to be set later
        faction_id = corp_data.get('factionID')
        description = corp_data.get('descriptionID', {}).get('en', '')
        
        # Create the row tuple in the correct order according to columns
        row = []
        for column in columns:
            if column == "corporation_id":
                row.append(corporation_id)
            elif column == "corporation_name":
                row.append(corporation_name)
            elif column == "ticker":
                row.append(ticker)
            elif column == "ceo_character_id":
                row.append(ceo_character_id)
            elif column == "tax_rate":
                row.append(tax_rate)
            elif column == "member_count":  # Using memberLimit as a proxy
                row.append(member_limit)
            elif column == "alliance_id":
                row.append(alliance_id)
            elif column == "faction_id":
                row.append(faction_id)
            elif column == "description":
                row.append(description)
            else:
                # For any other column, try to get it directly from corp_data
                row.append(corp_data.get(column))
        
        rows.append(tuple(row))
    
    return rows

def process_agents(data, columns):
    """Custom processor for agents data."""
    rows = []
    
    for agent_id, agent_data in data.items():
        row = []
        for column in columns:
            if column == "agent_id":
                row.append(agent_id)
            elif column == "is_locator":
                row.append(1 if agent_data.get('isLocator', False) else 0)
            elif column == "agent_type_id":
                row.append(agent_data.get('agentTypeID'))
            elif column == "corporation_id":
                row.append(agent_data.get('corporationID'))
            elif column == "division_id":
                row.append(agent_data.get('divisionID'))
            elif column == "level":
                row.append(agent_data.get('level'))
            elif column == "location_id":
                row.append(agent_data.get('locationID'))
            else:
                # For any other column, try to get it directly from agent_data
                row.append(agent_data.get(column))
        
        rows.append(tuple(row))
    
    return rows

def process_corporation_divisions(data, columns):
    """Custom processor for corporation divisions data."""
    rows = []
    
    for division_id, division_data in data.items():
        division_name = division_data.get('nameID', {}).get('en', 'Unknown')
        leader_type_name = division_data.get('leaderTypeNameID', {}).get('en', '')
        description = division_data.get('description', '')
        internal_name = division_data.get('internalName', '')
        
        row = []
        for column in columns:
            if column == "division_id":
                row.append(division_id)
            elif column == "division_name":
                row.append(division_name)
            elif column == "description":
                row.append(description)
            elif column == "leader_type_name":
                row.append(leader_type_name)
            elif column == "internal_name":
                row.append(internal_name)
            else:
                # For any other column, try to get it directly
                row.append(division_data.get(column))
        
        rows.append(tuple(row))
    
    return rows

def process_races(data, columns):
    """Custom processor for races data."""
    rows = []
    
    for race_id, race_data in data.items():
        race_name = race_data.get('nameID', {}).get('en', 'Unknown')
        description = race_data.get('descriptionID', {}).get('en', '')
        icon_id = race_data.get('iconID')
        ship_type_id = race_data.get('shipTypeID')
        
        row = []
        for column in columns:
            if column == "race_id":
                row.append(race_id)
            elif column == "race_name":
                row.append(race_name)
            elif column == "description":
                row.append(description)
            elif column == "icon_id":
                row.append(icon_id)
            elif column == "ship_type_id":
                row.append(ship_type_id)
            else:
                # For any other column, try to get it directly
                row.append(race_data.get(column))
        
        rows.append(tuple(row))
    
    return rows

def process_bloodlines(data, columns):
    """Custom processor for bloodlines data."""
    rows = []
    
    for bloodline_id, bloodline_data in data.items():
        bloodline_name = bloodline_data.get('nameID', {}).get('en', 'Unknown')
        description = bloodline_data.get('descriptionID', {}).get('en', '')
        race_id = bloodline_data.get('raceID')
        corporation_id = bloodline_data.get('corporationID')
        charisma = bloodline_data.get('charisma')
        intelligence = bloodline_data.get('intelligence')
        memory = bloodline_data.get('memory')
        perception = bloodline_data.get('perception')
        willpower = bloodline_data.get('willpower')
        
        row = []
        for column in columns:
            if column == "bloodline_id":
                row.append(bloodline_id)
            elif column == "bloodline_name":
                row.append(bloodline_name)
            elif column == "description":
                row.append(description)
            elif column == "race_id":
                row.append(race_id)
            elif column == "corporation_id":
                row.append(corporation_id)
            elif column == "charisma":
                row.append(charisma)
            elif column == "intelligence":
                row.append(intelligence)
            elif column == "memory":
                row.append(memory)
            elif column == "perception":
                row.append(perception)
            elif column == "willpower":
                row.append(willpower)
            else:
                # For any other column, try to get it directly
                row.append(bloodline_data.get(column))
        
        rows.append(tuple(row))
    
    return rows

def process_ancestries(data, columns):
    """Custom processor for ancestries data."""
    rows = []
    
    for ancestry_id, ancestry_data in data.items():
        ancestry_name = ancestry_data.get('nameID', {}).get('en', 'Unknown')
        description = ancestry_data.get('descriptionID', {}).get('en', '')
        bloodline_id = ancestry_data.get('bloodlineID')
        short_description = ancestry_data.get('shortDescription', '')
        icon_id = ancestry_data.get('iconID')
        
        row = []
        for column in columns:
            if column == "ancestry_id":
                row.append(ancestry_id)
            elif column == "ancestry_name":
                row.append(ancestry_name)
            elif column == "description":
                row.append(description)
            elif column == "bloodline_id":
                row.append(bloodline_id)
            elif column == "short_description":
                row.append(short_description)
            elif column == "icon_id":
                row.append(icon_id)
            else:
                # For any other column, try to get it directly
                row.append(ancestry_data.get(column))
        
        rows.append(tuple(row))
    
    return rows

def process_categories(data, columns):
    """Custom processor for categories data."""
    rows = []
    
    for category_id, category_data in data.items():
        category_name = category_data.get('name', {}).get('en', 'Unknown')
        published = 1 if category_data.get('published', False) else 0
        
        row = []
        for column in columns:
            if column == "category_id":
                row.append(category_id)
            elif column == "category_name":
                row.append(category_name)
            elif column == "published":
                row.append(published)
            else:
                # For any other column, try to get it directly
                row.append(category_data.get(column))
        
        rows.append(tuple(row))
    
    return rows

def process_groups(data, columns):
    """Custom processor for groups data."""
    rows = []
    
    for group_id, group_data in data.items():
        group_name = group_data.get('name', {}).get('en', 'Unknown')
        category_id = group_data.get('categoryID')
        published = 1 if group_data.get('published', False) else 0
        anchorable = 1 if group_data.get('anchorable', False) else 0
        anchored = 1 if group_data.get('anchored', False) else 0
        fittable_non_singleton = 1 if group_data.get('fittableNonSingleton', False) else 0
        
        row = []
        for column in columns:
            if column == "group_id":
                row.append(group_id)
            elif column == "group_name":
                row.append(group_name)
            elif column == "category_id":
                row.append(category_id)
            elif column == "published":
                row.append(published)
            elif column == "anchorable":
                row.append(anchorable)
            elif column == "anchored":
                row.append(anchored)
            elif column == "fittable_non_singleton":
                row.append(fittable_non_singleton)
            else:
                # For any other column, try to get it directly
                row.append(group_data.get(column))
        
        rows.append(tuple(row))
    
    return rows

def process_types(data, columns):
    """Custom processor for types data."""
    rows = []
    
    for type_id, type_data in data.items():
        type_name = type_data.get('name', {}).get('en', 'Unknown')
        group_id = type_data.get('groupID')
        published = 1 if type_data.get('published', False) else 0
        mass = type_data.get('mass')
        volume = type_data.get('volume')
        capacity = type_data.get('capacity')
        portion_size = type_data.get('portionSize')
        
        row = []
        for column in columns:
            if column == "type_id":
                row.append(type_id)
            elif column == "type_name":
                row.append(type_name)
            elif column == "group_id":
                row.append(group_id)
            elif column == "published":
                row.append(published)
            elif column == "mass":
                row.append(mass)
            elif column == "volume":
                row.append(volume)
            elif column == "capacity":
                row.append(capacity)
            elif column == "portion_size":
                row.append(portion_size)
            else:
                # For any other column, try to get it directly
                row.append(type_data.get(column))
        
        rows.append(tuple(row))
    
    return rows

def process_meta_groups(data, columns):
    """Custom processor for meta groups data."""
    rows = []
    
    for meta_group_id, meta_group_data in data.items():
        meta_group_name = meta_group_data.get('nameID', {}).get('en', 'Unknown')
        icon_id = meta_group_data.get('iconID')
        
        row = []
        for column in columns:
            if column == "meta_group_id":
                row.append(meta_group_id)
            elif column == "meta_group_name":
                row.append(meta_group_name)
            elif column == "icon_id":
                row.append(icon_id)
            else:
                # For any other column, try to get it directly
                row.append(meta_group_data.get(column))
        
        rows.append(tuple(row))
    
    return rows

def process_market_groups(data, columns):
    """Custom processor for market groups data."""
    rows = []
    
    for market_group_id, market_group_data in data.items():
        market_group_name = market_group_data.get('nameID', {}).get('en', 'Unknown')
        description = market_group_data.get('descriptionID', {}).get('en', '')
        icon_id = market_group_data.get('iconID')
        has_types = 1 if market_group_data.get('hasTypes', False) else 0
        
        row = []
        for column in columns:
            if column == "market_group_id":
                row.append(market_group_id)
            elif column == "market_group_name":
                row.append(market_group_name)
            elif column == "description":
                row.append(description)
            elif column == "icon_id":
                row.append(icon_id)
            elif column == "has_types":
                row.append(has_types)
            else:
                # For any other column, try to get it directly
                row.append(market_group_data.get(column))
        
        rows.append(tuple(row))
    
    return rows

def process_factions(data, columns):
    """Custom processor for factions data."""
    rows = []
    
    for faction_id, faction_data in data.items():
        faction_name = faction_data.get('nameID', {}).get('en', 'Unknown')
        description = faction_data.get('descriptionID', {}).get('en', '')
        corporation_id = faction_data.get('corporationID')
        militia_corporation_id = faction_data.get('militiaCorporationID')
        size_factor = faction_data.get('sizeFactor')
        station_count = 0  # Not directly available in SDE
        station_system_count = 0  # Not directly available in SDE
        is_unique = 1 if faction_data.get('uniqueName', False) else 0
        
        row = []
        for column in columns:
            if column == "faction_id":
                row.append(faction_id)
            elif column == "faction_name":
                row.append(faction_name)
            elif column == "description":
                row.append(description)
            elif column == "corporation_id":
                row.append(corporation_id)
            elif column == "militia_corporation_id":
                row.append(militia_corporation_id)
            elif column == "size_factor":
                row.append(size_factor)
            elif column == "station_count":
                row.append(station_count)
            elif column == "station_system_count":
                row.append(station_system_count)
            elif column == "is_unique":
                row.append(is_unique)
            else:
                # For any other column, try to get it directly
                row.append(faction_data.get(column))
        
        rows.append(tuple(row))
    
    return rows

def process_blueprints(data, columns):
    """Custom processor for blueprints data."""
    rows = []
    
    for blueprint_type_id, blueprint_data in data.items():
        max_production_limit = blueprint_data.get('maxProductionLimit')
        activities = blueprint_data.get('activities', {})
        
        copying_time = activities.get('copying', {}).get('time')
        manufacturing_time = activities.get('manufacturing', {}).get('time')
        research_material_time = activities.get('research_material', {}).get('time')
        research_time_time = activities.get('research_time', {}).get('time')
        
        row = []
        for column in columns:
            if column == "blueprint_type_id":
                row.append(blueprint_type_id)
            elif column == "max_production_limit":
                row.append(max_production_limit)
            elif column == "copying_time":
                row.append(copying_time)
            elif column == "manufacturing_time":
                row.append(manufacturing_time)
            elif column == "research_material_time":
                row.append(research_material_time)
            elif column == "research_time_time":
                row.append(research_time_time)
            else:
                # For any other column, try to get it directly
                row.append(blueprint_data.get(column))
        
        rows.append(tuple(row))
        
        # Process materials and products for manufacturing
        manufacturing = activities.get('manufacturing', {})
        if manufacturing and 'materials' in manufacturing:
            for material in manufacturing.get('materials', []):
                material_type_id = material.get('typeID')
                quantity = material.get('quantity')
                if material_type_id and quantity:
                    # These would go to a different table like eve_blueprint_materials
                    pass
        
        if manufacturing and 'products' in manufacturing:
            for product in manufacturing.get('products', []):
                product_type_id = product.get('typeID')
                quantity = product.get('quantity')
                if product_type_id and quantity:
                    # These would go to a different table like eve_blueprint_products
                    pass
    
    return rows

def process_type_materials(data, columns):
    """Custom processor for type materials data."""
    all_rows = []
    
    for type_id, type_data in data.items():
        materials = type_data.get('materials', [])
        
        for material in materials:
            material_type_id = material.get('materialTypeID')
            quantity = material.get('quantity')
            
            row = []
            for column in columns:
                if column == "type_id":
                    row.append(type_id)
                elif column == "material_type_id":
                    row.append(material_type_id)
                elif column == "quantity":
                    row.append(quantity)
                else:
                    # For any other column, try to get it directly
                    row.append(material.get(column))
            
            all_rows.append(tuple(row))
    
    return all_rows

def process_dogma_attributes(data, columns):
    """Custom processor for dogma attributes data."""
    rows = []
    
    for attribute_id, attribute_data in data.items():
        attribute_name = attribute_data.get('name', '')
        display_name = attribute_data.get('displayNameID', {}).get('en', '')
        category_id = attribute_data.get('categoryID')
        data_type = attribute_data.get('dataType')
        default_value = attribute_data.get('defaultValue')
        description = attribute_data.get('description', '')
        icon_id = attribute_data.get('iconID')
        unit_id = attribute_data.get('unitID')
        published = 1 if attribute_data.get('published', False) else 0
        stackable = 1 if attribute_data.get('stackable', False) else 0
        high_is_good = 1 if attribute_data.get('highIsGood', False) else 0
        
        row = []
        for column in columns:
            if column == "attribute_id":
                row.append(attribute_id)
            elif column == "attribute_name":
                row.append(attribute_name)
            elif column == "display_name":
                row.append(display_name)
            elif column == "category_id":
                row.append(category_id)
            elif column == "data_type":
                row.append(data_type)
            elif column == "default_value":
                row.append(default_value)
            elif column == "description":
                row.append(description)
            elif column == "icon_id":
                row.append(icon_id)
            elif column == "unit_id":
                row.append(unit_id)
            elif column == "published":
                row.append(published)
            elif column == "stackable":
                row.append(stackable)
            elif column == "high_is_good":
                row.append(high_is_good)
            else:
                # For any other column, try to get it directly
                row.append(attribute_data.get(column))
        
        rows.append(tuple(row))
    
    return rows

def process_dogma_effects(data, columns):
    """Custom processor for dogma effects data."""
    rows = []
    
    for effect_id, effect_data in data.items():
        effect_name = effect_data.get('effectName', '')
        effect_category = effect_data.get('effectCategory')
        is_offensive = 1 if effect_data.get('isOffensive', False) else 0
        is_assistance = 1 if effect_data.get('isAssistance', False) else 0
        duration_attribute_id = effect_data.get('durationAttributeID')
        discharge_attribute_id = effect_data.get('dischargeAttributeID')
        range_attribute_id = effect_data.get('rangeAttributeID')
        falloff_attribute_id = effect_data.get('falloffAttributeID')
        tracking_speed_attribute_id = effect_data.get('trackingSpeedAttributeID')
        fitting_usage_chance_attribute_id = effect_data.get('fittingUsageChanceAttributeID')
        resist_attribute_id = effect_data.get('resistanceAttributeID')
        published = 1 if effect_data.get('published', False) else 0
        electronic_chance = 1 if effect_data.get('electronicChance', False) else 0
        propulsion_chance = 1 if effect_data.get('propulsionChance', False) else 0
        
        row = []
        for column in columns:
            if column == "effect_id":
                row.append(effect_id)
            elif column == "effect_name":
                row.append(effect_name)
            elif column == "effect_category":
                row.append(effect_category)
            elif column == "is_offensive":
                row.append(is_offensive)
            elif column == "is_assistance":
                row.append(is_assistance)
            elif column == "duration_attribute_id":
                row.append(duration_attribute_id)
            elif column == "discharge_attribute_id":
                row.append(discharge_attribute_id)
            elif column == "range_attribute_id":
                row.append(range_attribute_id)
            elif column == "falloff_attribute_id":
                row.append(falloff_attribute_id)
            elif column == "tracking_speed_attribute_id":
                row.append(tracking_speed_attribute_id)
            elif column == "fitting_usage_chance_attribute_id":
                row.append(fitting_usage_chance_attribute_id)
            elif column == "resist_attribute_id":
                row.append(resist_attribute_id)
            elif column == "published":
                row.append(published)
            elif column == "electronic_chance":
                row.append(electronic_chance)
            elif column == "propulsion_chance":
                row.append(propulsion_chance)
            else:
                # For any other column, try to get it directly
                row.append(effect_data.get(column))
        
        rows.append(tuple(row))
    
    return rows

def main():
    if len(sys.argv) < 2:
        print("Usage: python sde_to_sql.py <sde_root_directory>")
        sys.exit(1)

    sde_dir = sys.argv[1]
    if not os.path.exists(sde_dir):
        print(f"SDE directory not found: {sde_dir}")
        sys.exit(1)

    # Create output directory
    output_dir = os.path.join(sde_dir, "sql")
    ensure_directory(output_dir)

    # Process different data types
    process_universe_data(sde_dir, output_dir)
    process_landmarks(sde_dir, output_dir)
    process_fsd_data(sde_dir, output_dir)
    process_bsd_data(sde_dir, output_dir)

    print(f"SQL generation complete. Files saved to {output_dir}")

if __name__ == "__main__":
    main()
    
    def process_fsd_data(sde_dir, output_dir):
        
        fsd_dir = os.path.join(sde_dir, "fsd")
        if not os.path.exists(fsd_dir):
         print(f"FSD directory not found: {fsd_dir}")
        return

    # Map of YAML files to table names and column lists
    fsd_mappings = {
        "agents.yaml": {
            "table": "eve_agents",
            "columns": ["agent_id", "corporation_id", "division_id", "level", "location_id", "agent_type_id", "is_locator"],
            "processor": "process_agents"
        },
        "npcCorporations.yaml": {
            "table": "eve_corporations",
            "columns": ["corporation_id", "corporation_name", "ticker", "ceo_character_id", "tax_rate", "member_count", "alliance_id", "faction_id", "description"],
            "processor": "process_npc_corporations"
        },
        "npcCorporationDivisions.yaml": {
            "table": "eve_corporation_divisions",
            "columns": ["division_id", "division_name", "description", "leader_type_name"],
            "processor": "process_corporation_divisions"
        },
        "races.yaml": {
            "table": "eve_races",
            "columns": ["race_id", "race_name", "description", "icon_id", "ship_type_id"],
            "processor": "process_races"
        },
        "bloodlines.yaml": {
            "table": "eve_bloodlines",
            "columns": ["bloodline_id", "bloodline_name", "race_id", "description", "corporation_id", "charisma", "intelligence", "memory", "perception", "willpower"],
            "processor": "process_bloodlines"
        },
        "ancestries.yaml": {
            "table": "eve_ancestries",
            "columns": ["ancestry_id", "ancestry_name", "bloodline_id", "description", "short_description", "icon_id"],
            "processor": "process_ancestries"
        },
        "categories.yaml": {
            "table": "eve_categories",
            "columns": ["category_id", "category_name", "published"],
            "processor": "process_categories"
        },
        "groups.yaml": {
            "table": "eve_groups",
            "columns": ["group_id", "group_name", "category_id", "published", "anchorable", "anchored", "fittable_non_singleton"],
            "processor": "process_groups"
        },
        "types.yaml": {
            "table": "eve_item_types",
            "columns": ["type_id", "type_name", "group_id", "published", "mass", "portion_size", "volume", "capacity"],
            "processor": "process_types"
        },
        "metaGroups.yaml": {
            "table": "eve_meta_groups",
            "columns": ["meta_group_id", "meta_group_name", "icon_id"],
            "processor": "process_meta_groups"
        },
        "marketGroups.yaml": {
            "table": "eve_market_groups",
            "columns": ["market_group_id", "market_group_name", "description", "icon_id", "has_types"],
            "processor": "process_market_groups"
        },
        "factions.yaml": {
            "table": "eve_factions",
            "columns": ["faction_id", "faction_name", "description", "corporation_id", "militia_corporation_id", "size_factor", "station_count", "station_system_count", "is_unique"],
            "processor": "process_factions"
        },
        "blueprints.yaml": {
            "table": "eve_blueprints",
            "columns": ["blueprint_type_id", "max_production_limit", "copying_time", "manufacturing_time", "research_material_time", "research_time_time"],
            "processor": "process_blueprints"
        },
        "typeMaterials.yaml": {
            "table": "eve_type_materials",
            "columns": ["type_id", "material_type_id", "quantity"],
            "processor": "process_type_materials"
        },
        "dogmaAttributes.yaml": {
            "table": "eve_dogma_attributes",
            "columns": ["attribute_id", "attribute_name", "category_id", "data_type", "default_value", "description", "icon_id", "unit_id", "published", "display_name", "stackable", "high_is_good"],
            "processor": "process_dogma_attributes"
        },
        "dogmaEffects.yaml": {
            "table": "eve_dogma_effects",
            "columns": ["effect_id", "effect_name", "effect_category", "is_offensive", "is_assistance", "duration_attribute_id", "discharge_attribute_id", "range_attribute_id", "falloff_attribute_id", "tracking_speed_attribute_id", "fitting_usage_chance_attribute_id", "resist_attribute_id", "electronic_chance", "propulsion_chance", "published"],
            "processor": "process_dogma_effects"
        },
        "typeDogma.yaml": {
            "table": "eve_type_dogma",
            "columns": ["type_id", "attribute_id", "value"],
            "processor": "process_type_dogma"
        }
        # Add more mappings as needed
    }

    for yaml_file, mapping in fsd_mappings.items():
        file_path = os.path.join(fsd_dir, yaml_file)
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            continue

        with open(file_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)

        rows = []
        
        # Check if there's a custom processor for this mapping
        if "processor" in mapping and hasattr(sys.modules[__name__], mapping["processor"]):
            # Call the custom processor
            processor_func = getattr(sys.modules[__name__], mapping["processor"])
            rows = processor_func(data, mapping["columns"])
        else:
            # Default processing
            for item_id, item_data in data.items():
                # Extract values based on column mapping
                row = []
                for column in mapping["columns"]:
                    if column == mapping["columns"][0]:  # Assuming first column is the ID
                        row.append(item_id)
                    elif column in item_data:
                        value = item_data[column]
                        if isinstance(value, dict) and 'en' in value:
                            value = value['en']
                        row.append(value)
                    else:
                        row.append(None)
                rows.append(tuple(row))

        # Create subdirectory based on table type (e.g. fsd, bsd)
        table_dir = os.path.join(output_dir, "fsd")
        ensure_directory(table_dir)
        
        write_sql_file(
            os.path.join(table_dir, f"{mapping['table']}.sql"),
            mapping["table"],
            mapping["columns"],
            rows
        )

def process_bsd_data(sde_dir, output_dir):
    """Process BSD (Binary Static Data) files."""
    bsd_dir = os.path.join(sde_dir, "bsd")
    if not os.path.exists(bsd_dir):
        print(f"BSD directory not found: {bsd_dir}")
        return

    # Map of YAML files to table names and column lists
    bsd_mappings = {
        "invNames.yaml": {
            "table": "eve_inv_names",
            "columns": ["item_id", "item_name"],
            "processor": "process_inv_names"
        },
        "staStations.yaml": {
            "table": "eve_stations",
            "columns": ["station_id", "station_name", "corporation_id", "system_id", "constellation_id", "region_id", "station_type_id", "x", "y", "z", "security", "docking_cost_per_volume", "max_ship_volume_dockable", "office_rental_cost"],
            "processor": "process_stations"
        },
        "invFlags.yaml": {
            "table": "eve_inv_flags",
            "columns": ["flag_id", "flag_name", "flag_text", "order_id"],
            "processor": "process_inv_flags"
        },
        "invItems.yaml": {
            "table": "eve_inv_items",
            "columns": ["item_id", "type_id", "owner_id", "location_id", "flag_id", "quantity"],
            "processor": "process_inv_items"
        },
        "invPositions.yaml": {
            "table": "eve_inv_positions",
            "columns": ["item_id", "x", "y", "z", "yaw", "pitch", "roll"],
            "processor": "process_inv_positions"
        },
        "invUniqueNames.yaml": {
            "table": "eve_inv_unique_names",
            "columns": ["item_id", "item_name", "group_id"],
            "processor": "process_inv_unique_names"
        }
        # Add more mappings as needed
    }

    for yaml_file, mapping in bsd_mappings.items():
        file_path = os.path.join(bsd_dir, yaml_file)
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            continue

        with open(file_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)

        rows = []
        
        # Check if there's a custom processor for this mapping
        if "processor" in mapping and hasattr(sys.modules[__name__], mapping["processor"]):
            # Call the custom processor
            processor_func = getattr(sys.modules[__name__], mapping["processor"])
            rows = processor_func(data, mapping["columns"])
        else:
            # Default processing (fallback)
            for item in data:
                # Extract values based on column mapping
                row = []
                for column in mapping["columns"]:
                    key = column
                    # Convert snake_case to camelCase if needed
                    if column in ["item_id", "item_name"]:
                        key = column.replace("item_", "")
                    row.append(item.get(key))
                rows.append(tuple(row))

        # Create subdirectory based on table type (e.g. fsd, bsd)
        table_dir = os.path.join(output_dir, "bsd")
        ensure_directory(table_dir)
        
        write_sql_file(
            os.path.join(table_dir, f"{mapping['table']}.sql"),
            mapping["table"],
            mapping["columns"],
            rows
        )

def write_sql_file(output_file, table_name, columns, rows):
    """Write SQL data to a file, splitting into multiple files if needed."""
    if not rows:
        print(f"No data to write for table {table_name}")
        return

    # Calculate number of files needed
    num_files = math.ceil(len(rows) / MAX_ROWS_PER_FILE)
    
    for file_num in range(num_files):
        start_idx = file_num * MAX_ROWS_PER_FILE
        end_idx = min((file_num + 1) * MAX_ROWS_PER_FILE, len(rows))
        
        # Get file name with part number if multiple files
        if num_files > 1:
            file_parts = os.path.splitext(output_file)
            file_part = f"{file_parts[0]}_{file_num+1}{file_parts[1]}"
        else:
            file_part = output_file
        
        with open(file_part, 'w', encoding='utf-8') as f:
            f.write(f"-- Generated by SDE to SQL converter on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("START TRANSACTION;\n\n")
            
            # Only include DELETE statement in first file
            if file_num == 0:
                f.write(f"-- Clear existing data\n")
                f.write(f"DELETE FROM {table_name};\n\n")
            
            f.write(f"-- Insert data part {file_num+1} of {num_files}\n")
            f.write(f"INSERT INTO {table_name} ({', '.join(columns)})\n")
            f.write("VALUES\n")
            
            # Write values
            for i, row in enumerate(rows[start_idx:end_idx]):
                values = [sanitize_value(val) for val in row]
                ending = ",\n" if i < end_idx - start_idx - 1 else ";\n"
                f.write(f"({', '.join(values)}){ending}")
            
            f.write("\nCOMMIT;\n")
        
        print(f"Wrote {end_idx - start_idx} rows to {file_part}")
        """ EVE Online SDE to SQL Converter
This script processes the EVE Online Static Data Export (SDE) and generates SQL files
for updating the zenevan_cms database. It walks through the SDE directory, reads
YAML files, and transforms them into SQL insert statements.

Usage:
    python sde_to_sql.py <sde_root_directory>
"""