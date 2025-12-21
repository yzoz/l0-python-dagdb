// Обновляем confirm_count для всех узлов
MATCH (t:Transaction)
OPTIONAL MATCH ()-[r:CONFIRMS]->(t)
WITH t, count(r) AS confirm_count
SET t.confirm_count = confirm_count
WITH collect(t) AS updated_nodes
// Собираем граф для визуализации
UNWIND updated_nodes AS t
MATCH (t)-[r:CONFIRMS]->(t2:Transaction)
RETURN t, r, t2

// AsNumber(Property(node, "confirm_count"))