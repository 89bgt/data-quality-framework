# Sample Table Schemas
# Copy this to config/schemas.py and update with your actual table schemas

TABLE_SCHEMAS = {
    "tva_due": {
        "date_insertion": "timestamp",
        "batch_id": "int",
        "systeme_source": "string",
        "montant_tva": "decimal",
        "numero_facture": "string",
        "date_facture": "date"
    },
    
    "dts_recap": {
        "date_insertion": "timestamp", 
        "batch_id": "int",
        "systeme_source": "string",
        "numero_dossier": "string",
        "statut_dossier": "string",
        "montant_total": "decimal"
    },
    
    "titre_participation": {
        "date_insertion": "timestamp",
        "batch_id": "int", 
        "systeme_source": "string",
        "numero_titre": "string",
        "type_participation": "string",
        "montant_participation": "decimal"
    }
}

def get_table_schema(table_name):
    """
    Get the expected schema for a specific table.
    
    Args:
        table_name (str): Name of the table
        
    Returns:
        dict: Schema definition with column names and types
    """
    return TABLE_SCHEMAS.get(table_name, {})

def get_all_schemas():
    """
    Get all table schemas.
    
    Returns:
        dict: All table schemas
    """
    return TABLE_SCHEMAS
