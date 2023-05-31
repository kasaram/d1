SELECT
  t.id,
  t.emp_name,
  t.social_field,
  COALESCE(json_extract_path_text(f.value, 'custinfo.custinfo.customer_id'), '0') AS customer_id
FROM
  your_table t,
  LATERAL FLATTEN(input => parse_json(t.social_field)) f;
