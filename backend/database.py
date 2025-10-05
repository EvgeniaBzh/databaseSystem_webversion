import os
import re
import json
from typing import Any, Dict, List, Optional
from sqlalchemy import create_engine, Column, Integer, String, Text, Boolean, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, scoped_session
from sqlalchemy.pool import StaticPool

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://dbms_user:dbms_password@localhost:5432/dbms')

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = scoped_session(sessionmaker(bind=engine))
Base = declarative_base()

# ==================== МОДЕЛІ БД ====================

class DatabaseModel(Base):
    __tablename__ = 'databases'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255), unique=True, nullable=False)
    
    tables = relationship('TableModel', back_populates='database', cascade='all, delete-orphan')

class TableModel(Base):
    __tablename__ = 'tables'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    database_id = Column(Integer, ForeignKey('databases.id', ondelete='CASCADE'), nullable=False)
    
    database = relationship('DatabaseModel', back_populates='tables')
    fields = relationship('FieldModel', back_populates='table', cascade='all, delete-orphan')
    records = relationship('RecordModel', back_populates='table', cascade='all, delete-orphan')

class FieldModel(Base):
    __tablename__ = 'fields'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    field_type = Column(String(50), nullable=False)
    is_primary_key = Column(Boolean, default=False)
    enum_values = Column(Text, nullable=True)
    position = Column(Integer, nullable=False)
    table_id = Column(Integer, ForeignKey('tables.id', ondelete='CASCADE'), nullable=False)
    
    table = relationship('TableModel', back_populates='fields')

class RecordModel(Base):
    __tablename__ = 'records'
    
    id = Column(Integer, primary_key=True)
    data = Column(Text, nullable=False)
    table_id = Column(Integer, ForeignKey('tables.id', ondelete='CASCADE'), nullable=False)
    
    table = relationship('TableModel', back_populates='records')

# Створюємо таблиці
Base.metadata.create_all(bind=engine)

# ==================== ТИПИ ДАНИХ ====================

class DataType:
    def __init__(self, name: str):
        self.name = name
    
    def validate(self, value: str) -> bool:
        return True
    
    def convert(self, value: str) -> Any:
        return value

class IntegerType(DataType):
    def __init__(self):
        super().__init__("integer")
    
    def validate(self, value: str) -> bool:
        try:
            int(value)
            return True
        except ValueError:
            return False
    
    def convert(self, value: str) -> int:
        return int(value)

class RealType(DataType):
    def __init__(self):
        super().__init__("real")
    
    def validate(self, value: str) -> bool:
        try:
            float(value)
            return True
        except ValueError:
            return False
    
    def convert(self, value: str) -> float:
        return float(value)

class CharType(DataType):
    def __init__(self):
        super().__init__("char")
    
    def validate(self, value: str) -> bool:
        return len(value) == 1
    
    def convert(self, value: str) -> str:
        return value

class StringType(DataType):
    def __init__(self):
        super().__init__("string")
    
    def validate(self, value: str) -> bool:
        return isinstance(value, str)
    
    def convert(self, value: str) -> str:
        return value

class EmailType(DataType):
    def __init__(self):
        super().__init__("email")
    
    def validate(self, value: str) -> bool:
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return re.match(pattern, value) is not None
    
    def convert(self, value: str) -> str:
        return value

class EnumType(DataType):
    def __init__(self, values: List[str]):
        super().__init__("enum")
        self.values = values
    
    def validate(self, value: str) -> bool:
        return value in self.values
    
    def convert(self, value: str) -> str:
        return value

# ==================== БІЗНЕС-ЛОГІКА ====================

class Field:
    def __init__(self, name: str, data_type: DataType, is_primary_key: bool = False):
        self.name = name
        self.data_type = data_type
        self.is_primary_key = is_primary_key
    
    def to_dict(self) -> Dict:
        result = {
            'name': self.name,
            'type': self.data_type.name,
            'is_primary_key': self.is_primary_key
        }
        if isinstance(self.data_type, EnumType):
            result['enum_values'] = self.data_type.values
        return result
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'Field':
        type_name = data['type']
        if type_name == 'integer':
            data_type = IntegerType()
        elif type_name == 'real':
            data_type = RealType()
        elif type_name == 'char':
            data_type = CharType()
        elif type_name == 'string':
            data_type = StringType()
        elif type_name == 'email':
            data_type = EmailType()
        elif type_name == 'enum':
            data_type = EnumType(data['enum_values'])
        else:
            raise ValueError(f"Unknown type: {type_name}")
        
        return cls(data['name'], data_type, data['is_primary_key'])

class Table:
    def __init__(self, name: str, table_id: Optional[int] = None):
        self.name = name
        self.table_id = table_id
        self.fields: List[Field] = []
        self.records: List[Dict[str, Any]] = []
    
    def add_field(self, field: Field) -> bool:
        if field.is_primary_key:
            for existing_field in self.fields:
                if existing_field.is_primary_key:
                    return False
        
        self.fields.append(field)
        
        if self.records:
            for record in self.records:
                record[field.name] = None
        
        return True
    
    def remove_field(self, field_name: str) -> bool:
        self.fields = [f for f in self.fields if f.name != field_name]
        for record in self.records:
            if field_name in record:
                del record[field_name]
        return True
    
    def add_record(self, record_data: Dict[str, str]) -> bool:
        validated_record = {}
        
        for field in self.fields:
            if field.name not in record_data:
                return False
            
            value = record_data[field.name]
            
            if not value.strip() and not field.is_primary_key:
                validated_record[field.name] = None
                continue
            
            if field.is_primary_key and not value.strip():
                return False
            
            if not field.data_type.validate(value):
                return False
            
            if field.is_primary_key:
                converted_value = field.data_type.convert(value)
                for existing_record in self.records:
                    if existing_record.get(field.name) == converted_value:
                        return False
            
            validated_record[field.name] = field.data_type.convert(value)
        
        self.records.append(validated_record)
        return True
    
    def update_record(self, record_index: int, record_data: Dict[str, str]) -> bool:
        if record_index < 0 or record_index >= len(self.records):
            return False
        
        validated_record = {}
        
        for field in self.fields:
            if field.name not in record_data:
                return False
            
            value = record_data[field.name]
            
            if not value.strip() and not field.is_primary_key:
                validated_record[field.name] = None
                continue
            
            if field.is_primary_key and not value.strip():
                return False
            
            if not field.data_type.validate(value):
                return False
            
            if field.is_primary_key:
                converted_value = field.data_type.convert(value)
                for i, existing_record in enumerate(self.records):
                    if i != record_index and existing_record.get(field.name) == converted_value:
                        return False
            
            validated_record[field.name] = field.data_type.convert(value)
        
        self.records[record_index] = validated_record
        return True
    
    def delete_record(self, record_index: int) -> bool:
        if record_index < 0 or record_index >= len(self.records):
            return False
        
        del self.records[record_index]
        return True
    
    def to_dict(self) -> Dict:
        return {
            'name': self.name,
            'fields': [field.to_dict() for field in self.fields],
            'records': self.records
        }

class Database:
    def __init__(self, name: str, db_id: Optional[int] = None):
        self.name = name
        self.db_id = db_id
        self.tables: List[Table] = []
    
    def add_table(self, table: Table) -> bool:
        for existing_table in self.tables:
            if existing_table.name == table.name:
                return False
        self.tables.append(table)
        return True
    
    def remove_table(self, table_name: str) -> bool:
        self.tables = [t for t in self.tables if t.name != table_name]
        return True
    
    def get_table(self, table_name: str) -> Optional[Table]:
        for table in self.tables:
            if table.name == table_name:
                return table
        return None
    
    def intersect_tables(self, table1_name: str, table2_name: str) -> Optional[Table]:
        table1 = self.get_table(table1_name)
        table2 = self.get_table(table2_name)
        
        if not table1 or not table2:
            return None
        
        if len(table1.fields) != len(table2.fields):
            return None
        
        for i, field1 in enumerate(table1.fields):
            field2 = table2.fields[i]
            if field1.name != field2.name or field1.data_type.name != field2.data_type.name:
                return None
        
        result_table = Table(f"{table1_name}_intersect_{table2_name}")
        for field in table1.fields:
            result_table.fields.append(Field(
                field.name,
                field.data_type,
                field.is_primary_key
            ))
        
        for record1 in table1.records:
            for record2 in table2.records:
                if record1 == record2:
                    result_table.records.append(record1.copy())
                    break
        
        return result_table

class DatabaseSystem:
    def __init__(self):
        self.session = SessionLocal()
    
    def get_databases(self) -> List[Database]:
        db_models = self.session.query(DatabaseModel).all()
        databases = []
        for db_model in db_models:
            db = Database(db_model.name, db_model.id)
            databases.append(db)
        return databases
    
    def add_database(self, database: Database) -> bool:
        existing = self.session.query(DatabaseModel).filter_by(name=database.name).first()
        if existing:
            return False
        
        db_model = DatabaseModel(name=database.name)
        self.session.add(db_model)
        self.session.commit()
        database.db_id = db_model.id
        return True
    
    def remove_database(self, db_name: str) -> bool:
        db_model = self.session.query(DatabaseModel).filter_by(name=db_name).first()
        if not db_model:
            return False
        
        self.session.delete(db_model)
        self.session.commit()
        return True
    
    def get_database(self, db_name: str) -> Optional[Database]:
        db_model = self.session.query(DatabaseModel).filter_by(name=db_name).first()
        if not db_model:
            return None
        
        database = Database(db_model.name, db_model.id)
        
        for table_model in db_model.tables:
            table = Table(table_model.name, table_model.id)
            
            fields = sorted(table_model.fields, key=lambda f: f.position)
            for field_model in fields:
                enum_vals = json.loads(field_model.enum_values) if field_model.enum_values else None
                field_dict = {
                    'name': field_model.name,
                    'type': field_model.field_type,
                    'is_primary_key': field_model.is_primary_key
                }
                if enum_vals:
                    field_dict['enum_values'] = enum_vals
                
                field = Field.from_dict(field_dict)
                table.fields.append(field)
            
            for record_model in table_model.records:
                record = json.loads(record_model.data)
                table.records.append(record)
            
            database.tables.append(table)
        
        return database
    
    def update_database_name(self, old_name: str, new_name: str) -> bool:
        db_model = self.session.query(DatabaseModel).filter_by(name=old_name).first()
        if not db_model:
            return False
        
        existing = self.session.query(DatabaseModel).filter_by(name=new_name).first()
        if existing and existing.id != db_model.id:
            return False
        
        db_model.name = new_name
        self.session.commit()
        return True
    
    def add_table(self, db_name: str, table: Table) -> bool:
        db_model = self.session.query(DatabaseModel).filter_by(name=db_name).first()
        if not db_model:
            return False
        
        existing = self.session.query(TableModel).filter_by(
            name=table.name, database_id=db_model.id
        ).first()
        if existing:
            return False
        
        table_model = TableModel(name=table.name, database_id=db_model.id)
        self.session.add(table_model)
        self.session.commit()
        table.table_id = table_model.id
        return True
    
    def remove_table(self, db_name: str, table_name: str) -> bool:
        db_model = self.session.query(DatabaseModel).filter_by(name=db_name).first()
        if not db_model:
            return False
        
        table_model = self.session.query(TableModel).filter_by(
            name=table_name, database_id=db_model.id
        ).first()
        if not table_model:
            return False
        
        self.session.delete(table_model)
        self.session.commit()
        return True
    
    def update_table_name(self, db_name: str, old_name: str, new_name: str) -> bool:
        db_model = self.session.query(DatabaseModel).filter_by(name=db_name).first()
        if not db_model:
            return False
        
        table_model = self.session.query(TableModel).filter_by(
            name=old_name, database_id=db_model.id
        ).first()
        if not table_model:
            return False
        
        existing = self.session.query(TableModel).filter_by(
            name=new_name, database_id=db_model.id
        ).first()
        if existing and existing.id != table_model.id:
            return False
        
        table_model.name = new_name
        self.session.commit()
        return True
    
    def save_table(self, db_name: str, table: Table):
        db_model = self.session.query(DatabaseModel).filter_by(name=db_name).first()
        if not db_model:
            return
        
        table_model = self.session.query(TableModel).filter_by(
            name=table.name, database_id=db_model.id
        ).first()
        if not table_model:
            return
        
        self.session.query(FieldModel).filter_by(table_id=table_model.id).delete()
        self.session.query(RecordModel).filter_by(table_id=table_model.id).delete()
        
        for pos, field in enumerate(table.fields):
            enum_vals = json.dumps(field.data_type.values) if isinstance(field.data_type, EnumType) else None
            field_model = FieldModel(
                name=field.name,
                field_type=field.data_type.name,
                is_primary_key=field.is_primary_key,
                enum_values=enum_vals,
                position=pos,
                table_id=table_model.id
            )
            self.session.add(field_model)
        
        for record in table.records:
            record_model = RecordModel(
                data=json.dumps(record),
                table_id=table_model.id
            )
            self.session.add(record_model)
        
        self.session.commit()
    
    def close(self):
        self.session.close()