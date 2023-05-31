SELECT
  id,
  emp_name,
  social_field,
  COALESCE(json_extract_path_text(json_parse(social_field), 'custinfo', 'custinfo', 'customer_id'), '0') AS customer_id
FROM
  your_table;
