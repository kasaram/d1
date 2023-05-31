

SELECT
  id,
  emp_name,
  social_field,
  CASE
    WHEN PARSE_JSON(social_field)[1]:custinfo:custinfo:customer_id IS NOT NULL THEN PARSE_JSON(social_field)[1]:custinfo:custinfo:customer_id
    ELSE '0'
  END AS customer_id
FROM
  your_table;
