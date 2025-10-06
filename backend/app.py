from flask import Flask, request, jsonify
from flask_cors import CORS
import json
import os
import traceback
from typing import Any, Dict, List, Optional
import re

from database import (
    DataType, IntegerType, RealType, CharType, StringType, 
    EmailType, EnumType, Field, Table, Database, DatabaseSystem
)

app = Flask(__name__)
CORS(app)

# ==================== DATABASE ENDPOINTS ====================

@app.route('/api/databases', methods=['GET'])
def get_databases():
    db_system = None
    try:
        db_system = DatabaseSystem()
        databases = db_system.get_databases()
        return jsonify([{'name': db.name} for db in databases])
    except Exception as e:
        print(f"Error in get_databases: {str(e)}")
        print(traceback.format_exc())
        return jsonify({'error': str(e)}), 500
    finally:
        if db_system:
            db_system.close()

@app.route('/api/databases', methods=['POST'])
def create_database():
    db_system = None
    try:
        data = request.json
        name = data.get('name')
        
        if not name:
            return jsonify({'error': 'Database name is required'}), 400
        
        db_system = DatabaseSystem()
        database = Database(name)
        if db_system.add_database(database):
            return jsonify({'message': f'Database {name} created', 'name': name}), 201
        else:
            return jsonify({'error': 'Database already exists'}), 400
    except Exception as e:
        print(f"Error in create_database: {str(e)}")
        print(traceback.format_exc())
        return jsonify({'error': str(e)}), 500
    finally:
        if db_system:
            db_system.close()

@app.route('/api/databases/<db_name>', methods=['PUT'])
def update_database(db_name):
    db_system = None
    try:
        data = request.json
        new_name = data.get('name')
        
        if not new_name:
            return jsonify({'error': 'New name is required'}), 400
        
        db_system = DatabaseSystem()
        if db_system.update_database_name(db_name, new_name):
            return jsonify({'message': f'Database renamed to {new_name}', 'name': new_name})
        else:
            return jsonify({'error': 'Database not found or name already exists'}), 400
    except Exception as e:
        print(f"Error in update_database: {str(e)}")
        print(traceback.format_exc())
        return jsonify({'error': str(e)}), 500
    finally:
        if db_system:
            db_system.close()

@app.route('/api/databases/<db_name>', methods=['DELETE'])
def delete_database(db_name):
    db_system = None
    try:
        db_system = DatabaseSystem()
        if db_system.remove_database(db_name):
            return jsonify({'message': f'Database {db_name} deleted'})
        else:
            return jsonify({'error': 'Database not found'}), 404
    except Exception as e:
        print(f"Error in delete_database: {str(e)}")
        print(traceback.format_exc())
        return jsonify({'error': str(e)}), 500
    finally:
        if db_system:
            db_system.close()

# ==================== TABLE ENDPOINTS ====================

@app.route('/api/databases/<db_name>/tables', methods=['GET'])
def get_tables(db_name):
    db_system = None
    try:
        db_system = DatabaseSystem()
        database = db_system.get_database(db_name)
        if not database:
            return jsonify({'error': 'Database not found'}), 404
        
        tables = [{'name': table.name} for table in database.tables]
        return jsonify(tables)
    except Exception as e:
        print(f"Error in get_tables: {str(e)}")
        print(traceback.format_exc())
        return jsonify({'error': str(e)}), 500
    finally:
        if db_system:
            db_system.close()

@app.route('/api/databases/<db_name>/tables', methods=['POST'])
def create_table(db_name):
    db_system = None
    try:
        data = request.json
        table_name = data.get('name')
        
        if not table_name:
            return jsonify({'error': 'Table name is required'}), 400
        
        db_system = DatabaseSystem()
        database = db_system.get_database(db_name)
        if not database:
            return jsonify({'error': 'Database not found'}), 404
        
        table = Table(table_name)
        if db_system.add_table(db_name, table):
            return jsonify({'message': f'Table {table_name} created', 'name': table_name}), 201
        else:
            return jsonify({'error': 'Table already exists'}), 400
    except Exception as e:
        print(f"Error in create_table: {str(e)}")
        print(traceback.format_exc())
        return jsonify({'error': str(e)}), 500
    finally:
        if db_system:
            db_system.close()

@app.route('/api/databases/<db_name>/tables/<table_name>', methods=['PUT'])
def update_table(db_name, table_name):
    db_system = None
    try:
        data = request.json
        new_name = data.get('name')
        
        if not new_name:
            return jsonify({'error': 'New name is required'}), 400
        
        db_system = DatabaseSystem()
        if db_system.update_table_name(db_name, table_name, new_name):
            return jsonify({'message': f'Table renamed to {new_name}', 'name': new_name})
        else:
            return jsonify({'error': 'Table not found or name already exists'}), 400
    except Exception as e:
        print(f"Error in update_table: {str(e)}")
        print(traceback.format_exc())
        return jsonify({'error': str(e)}), 500
    finally:
        if db_system:
            db_system.close()

@app.route('/api/databases/<db_name>/tables/<table_name>', methods=['DELETE'])
def delete_table(db_name, table_name):
    db_system = None
    try:
        db_system = DatabaseSystem()
        if db_system.remove_table(db_name, table_name):
            return jsonify({'message': f'Table {table_name} deleted'})
        else:
            return jsonify({'error': 'Table not found'}), 404
    except Exception as e:
        print(f"Error in delete_table: {str(e)}")
        print(traceback.format_exc())
        return jsonify({'error': str(e)}), 500
    finally:
        if db_system:
            db_system.close()

@app.route('/api/databases/<db_name>/tables/<table_name>', methods=['GET'])
def get_table_details(db_name, table_name):
    db_system = None
    try:
        db_system = DatabaseSystem()
        database = db_system.get_database(db_name)
        if not database:
            return jsonify({'error': 'Database not found'}), 404
        
        table = database.get_table(table_name)
        if not table:
            return jsonify({'error': 'Table not found'}), 404
        
        return jsonify(table.to_dict())
    except Exception as e:
        print(f"Error in get_table_details: {str(e)}")
        print(traceback.format_exc())
        return jsonify({'error': str(e)}), 500
    finally:
        if db_system:
            db_system.close()

# ==================== FIELD ENDPOINTS ====================

@app.route('/api/databases/<db_name>/tables/<table_name>/fields', methods=['POST'])
def add_field(db_name, table_name):
    db_system = None
    try:
        data = request.json
        field_name = data.get('name')
        field_type = data.get('type')
        is_primary_key = data.get('is_primary_key', False)
        enum_values = data.get('enum_values', [])
        
        if not field_name or not field_type:
            return jsonify({'error': 'Field name and type are required'}), 400
        
        db_system = DatabaseSystem()
        database = db_system.get_database(db_name)
        if not database:
            return jsonify({'error': 'Database not found'}), 404
        
        table = database.get_table(table_name)
        if not table:
            return jsonify({'error': 'Table not found'}), 404
        
        if field_type == 'integer':
            data_type = IntegerType()
        elif field_type == 'real':
            data_type = RealType()
        elif field_type == 'char':
            data_type = CharType()
        elif field_type == 'string':
            data_type = StringType()
        elif field_type == 'email':
            data_type = EmailType()
        elif field_type == 'enum':
            if not enum_values:
                return jsonify({'error': 'Enum values are required for enum type'}), 400
            data_type = EnumType(enum_values)
        else:
            return jsonify({'error': 'Invalid field type'}), 400
        
        field = Field(field_name, data_type, is_primary_key)
        
        if table.add_field(field):
            db_system.save_table(db_name, table)
            return jsonify({'message': f'Field {field_name} added', 'field': field.to_dict()}), 201
        else:
            return jsonify({'error': 'Primary key already exists or field cannot be added'}), 400
    except Exception as e:
        print(f"Error in add_field: {str(e)}")
        print(traceback.format_exc())
        return jsonify({'error': str(e)}), 500
    finally:
        if db_system:
            db_system.close()

@app.route('/api/databases/<db_name>/tables/<table_name>/fields/<field_name>', methods=['PUT'])
def update_field(db_name, table_name, field_name):
    db_system = None
    try:
        data = request.json
        new_name = data.get('name')
        new_type = data.get('type')
        is_primary_key = data.get('is_primary_key', False)
        enum_values = data.get('enum_values', [])
        
        if not new_name or not new_type:
            return jsonify({'error': 'Field name and type are required'}), 400
        
        db_system = DatabaseSystem()
        database = db_system.get_database(db_name)
        if not database:
            return jsonify({'error': 'Database not found'}), 404
        
        table = database.get_table(table_name)
        if not table:
            return jsonify({'error': 'Table not found'}), 404
        
        field_index = None
        for i, field in enumerate(table.fields):
            if field.name == field_name:
                field_index = i
                break
        
        if field_index is None:
            return jsonify({'error': 'Field not found'}), 404
        
        old_field = table.fields[field_index]
        
        if is_primary_key and not old_field.is_primary_key:
            for i, field in enumerate(table.fields):
                if i != field_index and field.is_primary_key:
                    return jsonify({'error': 'Primary key already exists'}), 400
        
        if new_type == 'integer':
            data_type = IntegerType()
        elif new_type == 'real':
            data_type = RealType()
        elif new_type == 'char':
            data_type = CharType()
        elif new_type == 'string':
            data_type = StringType()
        elif new_type == 'email':
            data_type = EmailType()
        elif new_type == 'enum':
            if not enum_values:
                return jsonify({'error': 'Enum values are required for enum type'}), 400
            data_type = EnumType(enum_values)
        else:
            return jsonify({'error': 'Invalid field type'}), 400
        
        table.fields[field_index] = Field(new_name, data_type, is_primary_key)
        
        if field_name != new_name or old_field.data_type.name != new_type:
            records_to_remove = []
            for i, record in enumerate(table.records):
                if field_name in record:
                    old_value = record.get(field_name)
                    
                    if field_name != new_name:
                        record[new_name] = record.pop(field_name)
                        old_value = record[new_name]
                    
                    if old_field.data_type.name != new_type:
                        if old_value is None:
                            record[new_name] = None
                        else:
                            str_value = str(old_value)
                            if data_type.validate(str_value):
                                try:
                                    record[new_name] = data_type.convert(str_value)
                                except:
                                    records_to_remove.append(i)
                            else:
                                records_to_remove.append(i)
            
            for i in reversed(records_to_remove):
                table.records.pop(i)
            
            if records_to_remove:
                warning_msg = f'Field updated. Warning: {len(records_to_remove)} record(s) were removed due to type incompatibility.'
            else:
                warning_msg = 'Field updated successfully'
        else:
            warning_msg = 'Field updated successfully'
            records_to_remove = []
        
        db_system.save_table(db_name, table)
        return jsonify({
            'message': warning_msg,
            'field': table.fields[field_index].to_dict(),
            'records_removed': len(records_to_remove)
        })
    except Exception as e:
        print(f"Error in update_field: {str(e)}")
        print(traceback.format_exc())
        return jsonify({'error': str(e)}), 500
    finally:
        if db_system:
            db_system.close()

@app.route('/api/databases/<db_name>/tables/<table_name>/fields/<field_name>', methods=['DELETE'])
def delete_field(db_name, table_name, field_name):
    db_system = None
    try:
        db_system = DatabaseSystem()
        database = db_system.get_database(db_name)
        if not database:
            return jsonify({'error': 'Database not found'}), 404
        
        table = database.get_table(table_name)
        if not table:
            return jsonify({'error': 'Table not found'}), 404
        
        if table.remove_field(field_name):
            db_system.save_table(db_name, table)
            return jsonify({'message': f'Field {field_name} deleted'})
        else:
            return jsonify({'error': 'Field not found'}), 404
    except Exception as e:
        print(f"Error in delete_field: {str(e)}")
        print(traceback.format_exc())
        return jsonify({'error': str(e)}), 500
    finally:
        if db_system:
            db_system.close()

# ==================== RECORD ENDPOINTS ====================

@app.route('/api/databases/<db_name>/tables/<table_name>/records', methods=['POST'])
def add_record(db_name, table_name):
    db_system = None
    try:
        data = request.json
        
        db_system = DatabaseSystem()
        database = db_system.get_database(db_name)
        if not database:
            return jsonify({'error': 'Database not found'}), 404
        
        table = database.get_table(table_name)
        if not table:
            return jsonify({'error': 'Table not found'}), 404
        
        if table.add_record(data):
            db_system.save_table(db_name, table)
            return jsonify({'message': 'Record added successfully'}), 201
        else:
            return jsonify({'error': 'Invalid record data or primary key violation'}), 400
    except Exception as e:
        print(f"Error in add_record: {str(e)}")
        print(traceback.format_exc())
        return jsonify({'error': str(e)}), 500
    finally:
        if db_system:
            db_system.close()

@app.route('/api/databases/<db_name>/tables/<table_name>/records/<int:record_index>', methods=['PUT'])
def update_record(db_name, table_name, record_index):
    db_system = None
    try:
        data = request.json
        
        db_system = DatabaseSystem()
        database = db_system.get_database(db_name)
        if not database:
            return jsonify({'error': 'Database not found'}), 404
        
        table = database.get_table(table_name)
        if not table:
            return jsonify({'error': 'Table not found'}), 404
        
        if table.update_record(record_index, data):
            db_system.save_table(db_name, table)
            return jsonify({'message': 'Record updated successfully'})
        else:
            return jsonify({'error': 'Invalid record data or record not found'}), 400
    except Exception as e:
        print(f"Error in update_record: {str(e)}")
        print(traceback.format_exc())
        return jsonify({'error': str(e)}), 500
    finally:
        if db_system:
            db_system.close()

@app.route('/api/databases/<db_name>/tables/<table_name>/records/<int:record_index>', methods=['DELETE'])
def delete_record(db_name, table_name, record_index):
    db_system = None
    try:
        db_system = DatabaseSystem()
        database = db_system.get_database(db_name)
        if not database:
            return jsonify({'error': 'Database not found'}), 404
        
        table = database.get_table(table_name)
        if not table:
            return jsonify({'error': 'Table not found'}), 404
        
        if table.delete_record(record_index):
            db_system.save_table(db_name, table)
            return jsonify({'message': 'Record deleted successfully'})
        else:
            return jsonify({'error': 'Record not found'}), 404
    except Exception as e:
        print(f"Error in delete_record: {str(e)}")
        print(traceback.format_exc())
        return jsonify({'error': str(e)}), 500
    finally:
        if db_system:
            db_system.close()

# ==================== INTERSECTION ENDPOINT ====================

@app.route('/api/databases/<db_name>/intersect', methods=['POST'])
def intersect_tables(db_name):
    db_system = None
    try:
        data = request.json
        table1_name = data.get('table1')
        table2_name = data.get('table2')
        save_as = data.get('save_as')
        
        if not table1_name or not table2_name:
            return jsonify({'error': 'Both table names are required'}), 400
        
        db_system = DatabaseSystem()
        database = db_system.get_database(db_name)
        if not database:
            return jsonify({'error': 'Database not found'}), 404
        
        result_table = database.intersect_tables(table1_name, table2_name)
        
        if result_table is None:
            return jsonify({'error': 'Tables have different structure or not found'}), 400
        
        if save_as:
            result_table.name = save_as
            if db_system.add_table(db_name, result_table):
                db_system.save_table(db_name, result_table)
                return jsonify({
                    'message': f'Intersection saved as {save_as}',
                    'result': result_table.to_dict()
                })
            else:
                return jsonify({'error': 'Table with this name already exists'}), 400
        
        return jsonify({'result': result_table.to_dict()})
    except Exception as e:
        print(f"Error in intersect_tables: {str(e)}")
        print(traceback.format_exc())
        return jsonify({'error': str(e)}), 500
    finally:
        if db_system:
            db_system.close()

# ==================== IMPORT ENDPOINT ====================

@app.route('/api/databases/import', methods=['POST'])
def import_database():
    db_system = None
    try:
        data = request.json
        
        if not data or 'name' not in data:
            return jsonify({'error': 'Invalid JSON format: missing database name'}), 400
        
        db_name = data['name']
        tables_data = data.get('tables', [])
        
        db_system = DatabaseSystem()
        
        # Перевіряємо чи база даних вже існує
        existing_db = db_system.get_database(db_name)
        if existing_db:
            return jsonify({'error': f'Database "{db_name}" already exists'}), 400
        
        # Створюємо нову базу даних
        database = Database(db_name)
        if not db_system.add_database(database):
            return jsonify({'error': 'Failed to create database'}), 500
        
        # Імпортуємо таблиці
        imported_tables = 0
        for table_data in tables_data:
            try:
                table_name = table_data.get('name')
                if not table_name:
                    continue
                
                table = Table(table_name)
                
                # Додаємо поля
                fields_data = table_data.get('fields', [])
                for field_data in fields_data:
                    field_name = field_data.get('name')
                    field_type = field_data.get('type')
                    is_primary_key = field_data.get('is_primary_key', False)
                    enum_values = field_data.get('enum_values', [])
                    
                    if not field_name or not field_type:
                        continue
                    
                    # Створюємо тип даних
                    if field_type == 'integer':
                        data_type = IntegerType()
                    elif field_type == 'real':
                        data_type = RealType()
                    elif field_type == 'char':
                        data_type = CharType()
                    elif field_type == 'string':
                        data_type = StringType()
                    elif field_type == 'email':
                        data_type = EmailType()
                    elif field_type == 'enum':
                        if not enum_values:
                            continue
                        data_type = EnumType(enum_values)
                    else:
                        continue
                    
                    field = Field(field_name, data_type, is_primary_key)
                    table.add_field(field)
                
                # Додаємо записи
                records_data = table_data.get('records', [])
                for record_data in records_data:
                    # Конвертуємо всі значення в рядки для валідації
                    str_record = {}
                    for key, value in record_data.items():
                        if value is None:
                            str_record[key] = ''
                        else:
                            str_record[key] = str(value)
                    
                    table.add_record(str_record)
                
                # Додаємо таблицю до БД
                if db_system.add_table(db_name, table):
                    db_system.save_table(db_name, table)
                    imported_tables += 1
                
            except Exception as table_error:
                print(f"Error importing table {table_data.get('name', 'unknown')}: {str(table_error)}")
                continue
        
        return jsonify({
            'message': f'Database "{db_name}" imported successfully',
            'tables_imported': imported_tables
        }), 201
        
    except json.JSONDecodeError as e:
        return jsonify({'error': f'Invalid JSON format: {str(e)}'}), 400
    except Exception as e:
        print(f"Error in import_database: {str(e)}")
        print(traceback.format_exc())
        return jsonify({'error': str(e)}), 500
    finally:
        if db_system:
            db_system.close()

# ==================== HEALTH CHECK ====================

@app.route('/api/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'ok', 'message': 'Database API is running'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)

