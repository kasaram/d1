SELECT
  id,
  emp_name,
  social_field,
  COALESCE(social_info:custinfo:custinfo:customer_id::string, '0') AS customer_id
FROM
  your_table,
  lateral flatten(input => parse_json(social_field)) f,
  lateral flatten(input => parse_json(f.value)) social_info;
