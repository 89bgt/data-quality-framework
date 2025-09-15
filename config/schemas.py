"""
Table schema definitions for data quality validation
"""

# Expected schemas for each table
TABLE_SCHEMAS = {
    "tva_due": {
        "date_insertion": "timestamp",
        "batch_id": "int",
        "systeme_source": "string",
        "type_import": "string",
        "id_declaration": "string",
        "id_adherent": "string",
        "identifiant_f": "string",
        "raison_sociale": "string",
        "code_declarant": "string",
        "date_depot": "timestamp",
        "type_declaration": "string",
        "exercice": "smallint",
        "id_ligne": "string",
        "credit": "float",
        "resolution": "string",
        "credit_apres": "float",
        "credit_accom": "float",
        "tva_due_per": "float",
        "net_paie": "float"
    },
    
    "dts_recap": {
        "date_insertion": "timestamp",
        "batch_id": "int",
        "systeme_source": "string",
        "type_import": "string",
        "id_formulaire": "string",
        "id_adherent": "string",
        "exercice_fiscal": "smallint",
        "date_depot": "timestamp",
        "num_depot": "bigint",
        "montant_per": "decimal(12,2)",
        "montant_spe": "decimal(12,2)",
        "montant_sf": "decimal(12,2)",
        "montant_apo": "decimal(12,2)",
        "societe_permi": "string",
        "nom_societe": "string",
        "id_ident": "string",
        "code_effectif": "tinyint",
        "nb_effectif": "smallint"
    },
    
    "titre_participation": {
        "date_insertion": "timestamp",
        "batch_id": "int",
        "systeme_source": "string",
        "type_import": "string",
        "type_liasse": "string",
        "id_formulaire": "string",
        "id_adherent": "string",
        "code_declarant": "string",
        "date_creation": "timestamp",
        "date_depot": "timestamp",
        "exercice_fiscal": "smallint",
        "exercice_fiscal_cl": "smallint",
        "id_ligne": "string",
        "nom_societe": "string",
        "secteur_activ": "string",
        "capital_social": "decimal(15,2)",
        "participation": "decimal(5,2)",
        "prix_achat": "decimal(15,2)",
        "prix_global": "decimal(15,2)"
    },

    "personnes": {
        "date_insertion": "timestamp",
        "nom": "string",
        "prenom": "string",
        "date_de_naissance": "timestamp",
        "profession": "string",
        "age": "int"
    },
    "table_test": {
        "date_insertion": "timestamp",
        "nom": "string",
        "prenom": "string",
        "date_de_naissance": "timestamp",
        "profession": "string",
        "age": "int"
    }

}

def get_schema(table_name):
    """
    Get the expected schema for a specific table
    
    Args:
        table_name (str): Name of the table
        
    Returns:
        dict: Schema definition or empty dict if not found
    """
    return TABLE_SCHEMAS.get(table_name, {})

def get_all_schemas():
    """
    Get all table schemas
    
    Returns:
        dict: All schema definitions
    """
    return TABLE_SCHEMAS

def add_schema(table_name, schema):
    """
    Add a new table schema
    
    Args:
        table_name (str): Name of the table
        schema (dict): Schema definition
    """
    TABLE_SCHEMAS[table_name] = schema

def list_tables():
    """
    Get list of all tables with defined schemas
    
    Returns:
        list: Table names
    """
    return list(TABLE_SCHEMAS.keys())
