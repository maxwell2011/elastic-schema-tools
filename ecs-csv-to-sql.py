"""
scripts/ecs-csv-to-sql.py
Script to convert the file located at Elastic/data/ecs.csv from a CSV into 
DDL, and generate preload statements for a database table
"""
import csv
from pathlib import Path
from typing import Any, Dict, List, OrderedDict

BASE_DIR = Path(__file__).parent.parent
ECS_DIR = BASE_DIR / "data" / "Elastic"
ECS_CSV_FILE = ECS_DIR / "ecs.csv"
ECS_SQL_FILE = ECS_DIR / "DDL-ecs.sql"
ECS_CSV_COLUMN_COUNT = 9
DEFAULT_SCHEMA_NAME = "ecs"
DEFAULT_TABLE_NAME = "elastic_log_schema"
DEFAULT_TABLE_OWNER = "postgres"

def _read_data(file: Path) -> list:
    """Read in the raw data from the local file containing a CSV dump of the given
    Elastic schema (https://github.com/elastic/ecs/blob/main/generated/csv/fields.csv)
    """
    with open(file, mode='r') as f:
        reader = csv.reader(f)
        data = [l for l in reader]
        f.close()
    return data

def _has_data(rawfile: list) -> bool:
    """Check if the CSV actually has any data that we're looking for
    """
    try:
        assert len(rawfile) > 1, "No data found in %s, Line Count: %s" % (str(ECS_CSV_FILE), str(len(rawfile)))
        return True
    except AssertionError as e:
        print(e)
    return False

def _has_right_column_number(rawfile: list) -> bool:
    """Make sure the right number of columns exist and we're
    not shooting our foot off
    """
    try:
        assert len(rawfile[0]) == ECS_CSV_COLUMN_COUNT, "Wrong header length in %s, Column Count: %s, expected %s" % (str(ECS_CSV_FILE), str(len(rawfile[0])), str(ECS_CSV_COLUMN_COUNT))
        return True
    except AssertionError as e:
        print(e)
    return False

def _format_data(rawfile: list) -> list:
    """Format the ingested CSV into a list of dicts
    """
    return [dict(zip(rawfile[0], l)) for l in rawfile[1:]]

def _clean_data(data: list) -> list:
    """Clean up the csv, convert '' values to None,
    and text booleans to python object bools
    """
    for d in data:
        d['Indexed'] = bool(d['Indexed'])
        if d["Normalization"] == '':
            d["Normalization"] = None
        if d["Example"] == '':
            d["Example"] = None
    return data

def readfile() -> List[Dict[str, Any]]:
    """Read in the csv and clean it up for processing
    """
    data = _read_data(ECS_CSV_FILE)
    if not _has_data(data):
        return []
    if not _has_right_column_number(data):
        return []
    data = _format_data(data)
    data = _clean_data(data)
    return data

def _add_constraint(schema_name:str, table_name:str, varname: str, data: List[Dict[str, Any]]) -> str:
    """Generate constraints for our table, based on what's in the CSV
    """
    CONSTRAINT = """CONSTRAINT "%s_%s_%s_oneof"\n\t\tCHECK (\n\t\t\t"%s" = ANY (ARRAY[""" % (schema_name, table_name, varname, varname)
    acceptable_items = set([d[varname] for d in data])
    for i,t in enumerate(acceptable_items):
        CONSTRAINT += f"""\n\t\t\t'{t}'::bpchar"""
        # There's a better solution to get the 'next-to-last' item
        # but I can't be bothered right now.
        if i < len(acceptable_items) - 1:
            CONSTRAINT += ""","""
        else:
            CONSTRAINT += """\n\t\t]))"""
            
    return CONSTRAINT
def _make_primary_key(fields: List[str]) -> str:
    return """PRIMARY KEY (""" + ", ".join([f'"{x}"' for x in fields]) + """)"""

def make_sql(data: List[Dict[str, Any]], schema_name: str, table_name: str, table_owner: str) -> str:
    """Actually build the table DDL statement
    """
    TABLE = """CREATE TABLE IF NOT EXISTS "%s"."%s" (""" % (schema_name, table_name)
    TABLE += """
        "ECS_Version" character(16) NOT NULL,
        "Indexed" boolean NOT NULL,
        "Field_Set" character(16) NOT NULL,
        "Field" character(96) NOT NULL,
        "Type" character(16) NOT NULL,
        "Level" character(16) NOT NULL,
        "Normalization" character(16),
        "Example" text,
        "Description" text,
        """
    TABLE += _make_primary_key(["ECS_Version", "Field_Set", "Field", "Type", "Level"])
    TABLE += ""","""
    constrained_fields = ["Type", "Field_Set", "Level"]
    for t in constrained_fields:
        TABLE += """\n\t"""
        TABLE += _add_constraint(schema_name, table_name, t, data)
        if t != constrained_fields[-1]:
            TABLE += ""","""
        TABLE += """\t"""
    TABLE += """\n\t);\n\n"""
    TABLE += """ALTER TABLE IF EXISTS "%s"."%s"\n\tOWNER to %s;""" % (schema_name, table_name, table_owner)
    return TABLE

def make_sql_preload(data: List[Dict[str, Any]], schema_name: str, table_name: str, table_owner: str) -> str:
    """Build a set of statements to preload the table we just generated
    """
    fields = ["ECS_Version", "Indexed", "Field_Set", "Field", "Type", "Level", "Normalization", "Example", "Description"]
    INSERT_STATEMENT = """\n\nINSERT INTO "%s"."%s"\n\t(\n\t\t""" % (schema_name, table_name)
    
    INSERT_STATEMENT += ", ".join([f'"{x}"' for x in fields])
    INSERT_STATEMENT += """\n\t)\n\t\tVALUES\n\t\t"""
    for i, d in enumerate(data):
        prepped = {}
        for k, v in d.items():
            if v is None:
                prepped[k] = "NULL"
            elif v == True:
                prepped[k] = "'true'"
            elif v == False:
                prepped[k] = "'false'"
            elif isinstance(v, str) and "'" in v:
                prepped[k] = """'%s'""" % (v.replace("'", ""), )
            else:
                prepped[k] = """'%s'""" % (v, )

        stmt = """, """.join([prepped[x] for x in fields])
        INSERT_STATEMENT += """("""
        INSERT_STATEMENT += stmt
        INSERT_STATEMENT += """)"""
        if i < len(data) - 1:
            INSERT_STATEMENT += ""","""
        INSERT_STATEMENT += """\n\t\t"""
    INSERT_STATEMENT += """;"""
    return INSERT_STATEMENT


def save_sql(sql: str, destfile: Path, verbose: bool = False) -> None:
    """Save the SQL we just generated
    """
    with open(destfile, 'w') as f:
        f.write(sql)
        f.close()


def main(schema_name: str, table_name: str, table_owner: str) -> None:
    """Read the file, build the DDL, print it to the terminal
    """
    data = readfile()
    sql = make_sql(data, schema_name, table_name, table_owner)
    sql += make_sql_preload(data, schema_name, table_name, table_owner)
    save_sql(sql, ECS_SQL_FILE, False)
    #print(sql)

if __name__ == "__main__":
    main(DEFAULT_SCHEMA_NAME, DEFAULT_TABLE_NAME, DEFAULT_TABLE_OWNER)
