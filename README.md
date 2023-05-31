SELECT
  id,
  emp_name,
  social_field,
  CASE
    WHEN social_field LIKE '%\"customer_id\":\"([A-Za-z0-9]+)\"%' THEN REGEXP_SUBSTR(social_field, '\"customer_id\":\"([A-Za-z0-9]+)\"', 1, 1, 'e')
    ELSE '0'
  END AS customer_id
FROM
  your_table;
