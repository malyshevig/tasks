import React from 'react';

const TableComponent = ({ data, loading, error }) => {
  if (loading) return <div>Загрузка...</div>;
  if (error) return <div>Ошибка: {error}</div>;

  if (!data || data.length === 0) {
    return <p>Нет данных</p>;
  }

  const headers = Object.keys(data[0]);
  const rows = data.map(item => Object.values(item));

  return (
    <table border="1" cellPadding="8" cellSpacing="0">
      <thead>
        <tr>
          {headers.map(header => (
            <th key={header}>{header}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {rows.map(row => (
          <tr key={row[0]}>
            {row.map(cell => (
              <td key={cell}>{cell}</td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
};

export default TableComponent;