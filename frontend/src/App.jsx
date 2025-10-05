import React, { useState, useEffect } from 'react';
import axios from 'axios';
import './App.css';

const API_URL = '';

function App() {
  const [databases, setDatabases] = useState([]);
  const [selectedDb, setSelectedDb] = useState(null);
  const [tables, setTables] = useState([]);
  const [selectedTable, setSelectedTable] = useState(null);
  const [tableDetails, setTableDetails] = useState(null);
  const [showModal, setShowModal] = useState(false);
  const [modalType, setModalType] = useState('');
  const [modalData, setModalData] = useState({});

  useEffect(() => {
    loadDatabases();
  }, []);

  useEffect(() => {
    if (selectedDb) {
      loadTables(selectedDb);
    }
  }, [selectedDb]);

  useEffect(() => {
    if (selectedDb && selectedTable) {
      loadTableDetails(selectedDb, selectedTable);
    }
  }, [selectedDb, selectedTable]);

  const loadDatabases = async () => {
    try {
      const response = await axios.get(`${API_URL}/api/databases`);
      setDatabases(response.data);
    } catch (error) {
      console.error('Помилка:', error);
      alert('Помилка завантаження баз даних: ' + (error.response?.data?.error || error.message));
    }
  };

  const loadTables = async (dbName) => {
    try {
      const response = await axios.get(`${API_URL}/api/databases/${dbName}/tables`);
      setTables(response.data);
    } catch (error) {
      console.error('Помилка:', error);
      alert('Помилка завантаження таблиць: ' + (error.response?.data?.error || error.message));
    }
  };

  const loadTableDetails = async (dbName, tableName) => {
    try {
      const response = await axios.get(`${API_URL}/api/databases/${dbName}/tables/${tableName}`);
      setTableDetails(response.data);
    } catch (error) {
      console.error('Помилка:', error);
      alert('Помилка завантаження деталей таблиці: ' + (error.response?.data?.error || error.message));
    }
  };

  const openModal = (type, data = {}) => {
    setModalType(type);
    setModalData(data);
    setShowModal(true);
  };

  const closeModal = () => {
    setShowModal(false);
    setModalType('');
    setModalData({});
  };

  const handleCreateDatabase = async (name) => {
    try {
      await axios.post(`${API_URL}/api/databases`, { name });
      loadDatabases();
      closeModal();
      alert('База даних створена!');
    } catch (error) {
      console.error('Помилка:', error);
      alert('Помилка: ' + (error.response?.data?.error || error.message));
    }
  };

  const handleEditDatabase = async (oldName, newName) => {
    try {
      await axios.put(`${API_URL}/api/databases/${oldName}`, { name: newName });
      if (selectedDb === oldName) {
        setSelectedDb(newName);
      }
      loadDatabases();
      closeModal();
      alert('База даних перейменована!');
    } catch (error) {
      console.error('Помилка:', error);
      alert('Помилка: ' + (error.response?.data?.error || error.message));
    }
  };

  const handleDeleteDatabase = async (dbName) => {
    if (window.confirm(`Видалити базу даних "${dbName}"?`)) {
      try {
        await axios.delete(`${API_URL}/api/databases/${dbName}`);
        if (selectedDb === dbName) {
          setSelectedDb(null);
          setTables([]);
          setSelectedTable(null);
          setTableDetails(null);
        }
        loadDatabases();
        alert('База даних видалена!');
      } catch (error) {
        console.error('Помилка:', error);
        alert('Помилка: ' + (error.response?.data?.error || error.message));
      }
    }
  };

  const handleCreateTable = async (name) => {
    try {
      await axios.post(`${API_URL}/api/databases/${selectedDb}/tables`, { name });
      loadTables(selectedDb);
      closeModal();
      alert('Таблиця створена!');
    } catch (error) {
      console.error('Помилка:', error);
      alert('Помилка: ' + (error.response?.data?.error || error.message));
    }
  };

  const handleEditTable = async (oldName, newName) => {
    try {
      await axios.put(`${API_URL}/api/databases/${selectedDb}/tables/${oldName}`, { name: newName });
      if (selectedTable === oldName) {
        setSelectedTable(newName);
      }
      loadTables(selectedDb);
      closeModal();
      alert('Таблиця перейменована!');
    } catch (error) {
      console.error('Помилка:', error);
      alert('Помилка: ' + (error.response?.data?.error || error.message));
    }
  };

  const handleDeleteTable = async (tableName) => {
    if (window.confirm(`Видалити таблицю "${tableName}"?`)) {
      try {
        await axios.delete(`${API_URL}/api/databases/${selectedDb}/tables/${tableName}`);
        if (selectedTable === tableName) {
          setSelectedTable(null);
          setTableDetails(null);
        }
        loadTables(selectedDb);
        alert('Таблиця видалена!');
      } catch (error) {
        console.error('Помилка:', error);
        alert('Помилка: ' + (error.response?.data?.error || error.message));
      }
    }
  };

  const handleAddField = async (fieldData) => {
    try {
      await axios.post(
        `${API_URL}/api/databases/${selectedDb}/tables/${selectedTable}/fields`,
        fieldData
      );
      loadTableDetails(selectedDb, selectedTable);
      closeModal();
      alert('Поле додано!');
    } catch (error) {
      console.error('Помилка:', error);
      alert('Помилка: ' + (error.response?.data?.error || error.message));
    }
  };

  const handleEditField = async (oldName, fieldData) => {
    try {
      await axios.put(
        `${API_URL}/api/databases/${selectedDb}/tables/${selectedTable}/fields/${oldName}`,
        fieldData
      );
      loadTableDetails(selectedDb, selectedTable);
      closeModal();
      alert('Поле оновлено!');
    } catch (error) {
      console.error('Помилка:', error);
      alert('Помилка: ' + (error.response?.data?.error || error.message));
    }
  };

  const handleDeleteField = async (fieldName) => {
    if (window.confirm(`Видалити поле "${fieldName}"?`)) {
      try {
        await axios.delete(
          `${API_URL}/api/databases/${selectedDb}/tables/${selectedTable}/fields/${fieldName}`
        );
        loadTableDetails(selectedDb, selectedTable);
        alert('Поле видалене!');
      } catch (error) {
        console.error('Помилка:', error);
        alert('Помилка: ' + (error.response?.data?.error || error.message));
      }
    }
  };

  const handleAddRecord = async (recordData) => {
    try {
      await axios.post(
        `${API_URL}/api/databases/${selectedDb}/tables/${selectedTable}/records`,
        recordData
      );
      loadTableDetails(selectedDb, selectedTable);
      closeModal();
      alert('Запис додано!');
    } catch (error) {
      console.error('Помилка:', error);
      alert('Помилка: ' + (error.response?.data?.error || error.message));
    }
  };

  const handleUpdateRecord = async (index, recordData) => {
    try {
      await axios.put(
        `${API_URL}/api/databases/${selectedDb}/tables/${selectedTable}/records/${index}`,
        recordData
      );
      loadTableDetails(selectedDb, selectedTable);
      closeModal();
      alert('Запис оновлено!');
    } catch (error) {
      console.error('Помилка:', error);
      alert('Помилка: ' + (error.response?.data?.error || error.message));
    }
  };

  const handleDeleteRecord = async (index) => {
    if (window.confirm('Видалити запис?')) {
      try {
        await axios.delete(
          `${API_URL}/api/databases/${selectedDb}/tables/${selectedTable}/records/${index}`
        );
        loadTableDetails(selectedDb, selectedTable);
        alert('Запис видалено!');
      } catch (error) {
        console.error('Помилка:', error);
        alert('Помилка: ' + (error.response?.data?.error || error.message));
      }
    }
  };

  const handleIntersectTables = async (table1, table2, saveAs) => {
    try {
      const payload = { table1, table2 };
      if (saveAs) {
        payload.save_as = saveAs;
      }
      
      const response = await axios.post(
        `${API_URL}/api/databases/${selectedDb}/intersect`,
        payload
      );
      
      if (saveAs) {
        loadTables(selectedDb);
        alert(`Результат перетину збережено як "${saveAs}"!`);
      } else {
        alert(`Перетин виконано успішно!\nЗнайдено записів: ${response.data.result.records.length}`);
      }
      closeModal();
    } catch (error) {
      console.error('Помилка:', error);
      alert('Помилка: ' + (error.response?.data?.error || error.message));
    }
  };

  return (
    <div className="app">
      <header className="app-header">
        <h1>Система управління табличними БД</h1>
      </header>

      <div className="main-container">
        {/* Панель баз даних */}
        <div className="panel databases-panel">
          <div className="panel-header">
            <h2>Бази даних</h2>
            <button onClick={() => openModal('createDatabase')}>Створити БД</button>
          </div>
          <div className="list">
            {databases.map((db) => (
              <div
                key={db.name}
                className={`list-item ${selectedDb === db.name ? 'selected' : ''}`}
                onClick={() => setSelectedDb(db.name)}
              >
                <span>{db.name}</span>
                <div className="item-actions">
                  <button
                    className="edit-btn-small"
                    onClick={(e) => {
                      e.stopPropagation();
                      openModal('editDatabase', { oldName: db.name });
                    }}
                  >
                    ✎
                  </button>
                  <button
                    className="delete-btn"
                    onClick={(e) => {
                      e.stopPropagation();
                      handleDeleteDatabase(db.name);
                    }}
                  >
                    ✕
                  </button>
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* Панель таблиць */}
        <div className="panel tables-panel">
          <div className="panel-header">
            <h2>Таблиці</h2>
            <div style={{ display: 'flex', gap: '8px' }}>
              <button onClick={() => openModal('createTable')} disabled={!selectedDb}>
                Створити таблицю
              </button>
              <button 
                onClick={() => openModal('intersectTables')} 
                disabled={!selectedDb || tables.length < 2}
                style={{ background: 'linear-gradient(135deg, #4299e1 0%, #3182ce 100%)' }}
              >
                ∩ Перетин
              </button>
            </div>
          </div>
          <div className="list">
            {tables.map((table) => (
              <div
                key={table.name}
                className={`list-item ${selectedTable === table.name ? 'selected' : ''}`}
                onClick={() => setSelectedTable(table.name)}
              >
                <span>{table.name}</span>
                <div className="item-actions">
                  <button
                    className="edit-btn-small"
                    onClick={(e) => {
                      e.stopPropagation();
                      openModal('editTable', { oldName: table.name });
                    }}
                  >
                    ✎
                  </button>
                  <button
                    className="delete-btn"
                    onClick={(e) => {
                      e.stopPropagation();
                      handleDeleteTable(table.name);
                    }}
                  >
                    ✕
                  </button>
                </div>
              </div>
            ))}
          </div>

          {/* Поля таблиці */}
          {selectedTable && tableDetails && (
            <div className="fields-section">
              <div className="panel-header">
                <h3>Поля таблиці</h3>
                <button onClick={() => openModal('addField')}>Додати поле</button>
              </div>
              <div className="list">
                {tableDetails.fields.map((field) => (
                  <div key={field.name} className={`list-item field-item ${field.is_primary_key ? 'primary-key-field' : ''}`}>
                    <span>
                      {field.is_primary_key && '🔑 '}
                      {field.name}: {field.type}
                      {field.is_primary_key && ' (PK)'}
                      {field.enum_values && ` (${field.enum_values.join(', ')})`}
                    </span>
                    <div className="item-actions">
                      <button
                        className="edit-btn-small"
                        onClick={() => openModal('editField', { field })}
                      >
                        ✎
                      </button>
                      <button
                        className="delete-btn"
                        onClick={() => handleDeleteField(field.name)}
                      >
                        ✕
                      </button>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>

        {/* Панель записів */}
        <div className="panel records-panel">
          <div className="panel-header">
            <h2>Записи таблиці</h2>
            <button
              onClick={() => openModal('addRecord')}
              disabled={!tableDetails || tableDetails.fields.length === 0}
            >
              Додати запис
            </button>
          </div>
          {tableDetails && tableDetails.fields.length > 0 && (
            <div className="records-table-container">
              <table className="records-table">
                <thead>
                  <tr>
                    {tableDetails.fields.map((field) => (
                      <th key={field.name}>{field.name}</th>
                    ))}
                    <th>Дії</th>
                  </tr>
                </thead>
                <tbody>
                  {tableDetails.records.map((record, index) => (
                    <tr key={index}>
                      {tableDetails.fields.map((field) => (
                        <td key={field.name}>
                          {record[field.name] === null ? 'NULL' : String(record[field.name])}
                        </td>
                      ))}
                      <td>
                        <button
                          className="action-btn edit-btn"
                          onClick={() => openModal('editRecord', { index, record })}
                        >
                          ✎
                        </button>
                        <button
                          className="action-btn delete-btn"
                          onClick={() => handleDeleteRecord(index)}
                        >
                          ✕
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>

      {/* Модальні вікна */}
      {showModal && (
        <Modal
          type={modalType}
          data={modalData}
          tableDetails={tableDetails}
          tables={tables}
          onClose={closeModal}
          onSubmit={(data) => {
            if (modalType === 'createDatabase') handleCreateDatabase(data.name);
            else if (modalType === 'editDatabase') handleEditDatabase(data.oldName, data.name);
            else if (modalType === 'createTable') handleCreateTable(data.name);
            else if (modalType === 'editTable') handleEditTable(data.oldName, data.name);
            else if (modalType === 'addField') handleAddField(data);
            else if (modalType === 'editField') handleEditField(data.oldName, data.fieldData);
            else if (modalType === 'addRecord') handleAddRecord(data);
            else if (modalType === 'editRecord') handleUpdateRecord(data.index, data.record);
            else if (modalType === 'intersectTables') handleIntersectTables(data.table1, data.table2, data.saveAs);
          }}
        />
      )}
    </div>
  );
}

// Модальне вікно
function Modal({ type, data, tableDetails, tables, onClose, onSubmit }) {
  const [formData, setFormData] = useState({});

  useEffect(() => {
    if (type === 'editDatabase') {
      setFormData({ name: data.oldName, oldName: data.oldName });
    } else if (type === 'editTable') {
      setFormData({ name: data.oldName, oldName: data.oldName });
    } else if (type === 'editRecord') {
      setFormData(data.record || {});
    } else if (type === 'addField') {
      setFormData({ name: '', type: 'string', is_primary_key: false, enum_values: '' });
    } else if (type === 'editField') {
      const field = data.field;
      setFormData({
        name: field.name,
        oldName: field.name,
        type: field.type,
        is_primary_key: field.is_primary_key,
        enum_values: field.enum_values ? field.enum_values.join(', ') : ''
      });
    } else if (type === 'intersectTables') {
      setFormData({ table1: '', table2: '', saveAs: '' });
    }
  }, [type, data]);

  const handleSubmit = (e) => {
    e.preventDefault();
    
    if (type === 'createDatabase' || type === 'createTable') {
      onSubmit({ name: formData.name });
    } else if (type === 'editDatabase' || type === 'editTable') {
      onSubmit({ oldName: formData.oldName, name: formData.name });
    } else if (type === 'addField') {
      const fieldData = {
        name: formData.name,
        type: formData.type,
        is_primary_key: formData.is_primary_key,
      };
      if (formData.type === 'enum') {
        fieldData.enum_values = formData.enum_values.split(',').map(v => v.trim()).filter(v => v);
      }
      onSubmit(fieldData);
    } else if (type === 'editField') {
      const fieldData = {
        name: formData.name,
        type: formData.type,
        is_primary_key: formData.is_primary_key,
      };
      if (formData.type === 'enum') {
        fieldData.enum_values = formData.enum_values.split(',').map(v => v.trim()).filter(v => v);
      }
      onSubmit({ oldName: formData.oldName, fieldData });
    } else if (type === 'addRecord') {
      onSubmit(formData);
    } else if (type === 'editRecord') {
      onSubmit({ index: data.index, record: formData });
    } else if (type === 'intersectTables') {
      onSubmit({ 
        table1: formData.table1, 
        table2: formData.table2, 
        saveAs: formData.saveAs 
      });
    }
  };

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <h2>
          {type === 'createDatabase' && 'Створити базу даних'}
          {type === 'editDatabase' && 'Редагувати базу даних'}
          {type === 'createTable' && 'Створити таблицю'}
          {type === 'editTable' && 'Редагувати таблицю'}
          {type === 'addField' && 'Додати поле'}
          {type === 'editField' && 'Редагувати поле'}
          {type === 'addRecord' && 'Додати запис'}
          {type === 'editRecord' && 'Редагувати запис'}
          {type === 'intersectTables' && 'Перетин таблиць'}
        </h2>
        
        <form onSubmit={handleSubmit}>
          {(type === 'createDatabase' || type === 'createTable' || type === 'editDatabase' || type === 'editTable') && (
            <input
              type="text"
              placeholder="Назва"
              value={formData.name || ''}
              onChange={(e) => setFormData({ ...formData, name: e.target.value })}
              required
            />
          )}

          {(type === 'addField' || type === 'editField') && (
            <>
              <input
                type="text"
                placeholder="Назва поля"
                value={formData.name || ''}
                onChange={(e) => setFormData({ ...formData, name: e.target.value })}
                required
              />
              <select
                value={formData.type || 'string'}
                onChange={(e) => setFormData({ ...formData, type: e.target.value })}
              >
                <option value="integer">Integer</option>
                <option value="real">Real</option>
                <option value="char">Char</option>
                <option value="string">String</option>
                <option value="email">Email</option>
                <option value="enum">Enum</option>
              </select>
              {formData.type === 'enum' && (
                <input
                  type="text"
                  placeholder="Значення enum (через кому)"
                  value={formData.enum_values || ''}
                  onChange={(e) => setFormData({ ...formData, enum_values: e.target.value })}
                />
              )}
              <label>
                <input
                  type="checkbox"
                  checked={formData.is_primary_key || false}
                  onChange={(e) => setFormData({ ...formData, is_primary_key: e.target.checked })}
                />
                Primary Key
              </label>
            </>
          )}

          {type === 'intersectTables' && tables && (
            <>
              <div className="form-group">
                <label>Перша таблиця:</label>
                <select
                  value={formData.table1 || ''}
                  onChange={(e) => setFormData({ ...formData, table1: e.target.value })}
                  required
                >
                  <option value="">-- Оберіть таблицю --</option>
                  {tables.map((table) => (
                    <option key={table.name} value={table.name}>{table.name}</option>
                  ))}
                </select>
              </div>
              <div className="form-group">
                <label>Друга таблиця:</label>
                <select
                  value={formData.table2 || ''}
                  onChange={(e) => setFormData({ ...formData, table2: e.target.value })}
                  required
                >
                  <option value="">-- Оберіть таблицю --</option>
                  {tables.map((table) => (
                    <option key={table.name} value={table.name}>{table.name}</option>
                  ))}
                </select>
              </div>
              <div className="form-group">
                <label>Зберегти як (опціонально):</label>
                <input
                  type="text"
                  placeholder="Назва нової таблиці"
                  value={formData.saveAs || ''}
                  onChange={(e) => setFormData({ ...formData, saveAs: e.target.value })}
                />
              </div>
            </>
          )}

          {(type === 'addRecord' || type === 'editRecord') && tableDetails && (
            <>
              {tableDetails.fields.map((field) => (
                <div key={field.name} className="form-group">
                  <label>
                    {field.name} ({field.type}){field.is_primary_key && ' (PK)'}:
                  </label>
                  {field.type === 'enum' ? (
                    <select
                      value={formData[field.name] || ''}
                      onChange={(e) => setFormData({ ...formData, [field.name]: e.target.value })}
                      required={field.is_primary_key}
                    >
                      <option value="">-- Оберіть значення --</option>
                      {field.enum_values.map((val) => (
                        <option key={val} value={val}>{val}</option>
                      ))}
                    </select>
                  ) : (
                    <input
                      type="text"
                      value={formData[field.name] || ''}
                      onChange={(e) => setFormData({ ...formData, [field.name]: e.target.value })}
                      required={field.is_primary_key}
                    />
                  )}
                </div>
              ))}
            </>
          )}

          <div className="modal-actions">
            <button type="submit">Зберегти</button>
            <button type="button" onClick={onClose}>Скасувати</button>
          </div>
        </form>
      </div>
    </div>
  );
}

export default App;