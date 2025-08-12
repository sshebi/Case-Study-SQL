CREATE TEMP FUNCTION 
extract_menu(menus_json STRING, 
menu_uuid STRING) 
RETURNS STRING 
LANGUAGE js AS """ const menus = JSON.parse(menus_json); return 
JSON.stringify(menus[menu_uuid] || {}); """; 
WITH 
matching_stores AS ( 
SELECT 
DISTINCT u.b_name, 
u.vb_name, 
JSON_VALUE(u.response, '$.data.menuMapping[0].menuUUID') AS menu_uuid 
FROM 
`arboreal-vision-339901.take_home_v2.virtual_kitchen_ubereats_hours` u 
INNER JOIN 
`arboreal-vision-339901.take_home_v2.virtual_kitchen_grubhub_hours` g 
ON 
u.b_name = g.b_name 
AND u.vb_name = g.vb_name 
WHERE 
JSON_VALUE(u.response, '$.data.menuMapping[0].menuUUID') IS NOT NULL ), 
menu_ids AS ( 
SELECT 
menu_uuid AS uuid 
FROM 
matching_stores 
GROUP BY 
menu_uuid 
ORDER BY 
COUNT(*) DESC ), 
latest_ubereats AS ( 
SELECT 
b_name, 
vb_name, 
MAX(timestamp) AS max_timestamp 
FROM 
`arboreal-vision-339901.take_home_v2.virtual_kitchen_ubereats_hours` 
GROUP BY 
b_name, 
vb_name ), 
ubereats_raw AS ( 
  SELECT 
    u.b_name, 
    u.vb_name, 
    u.slug AS ue_slug, 
    m.uuid AS menu_uuid, 
    extract_menu(TO_JSON_STRING(JSON_EXTRACT(u.response, '$.data.menus')), 
      m.uuid) AS menus_string 
  FROM 
    `arboreal-vision-339901.take_home_v2.virtual_kitchen_ubereats_hours` u 
  JOIN 
    menu_ids m 
  ON 
    JSON_VALUE(u.response, '$.data.menuMapping[0].menuUUID') = m.uuid 
  JOIN 
    latest_ubereats l 
  ON 
    u.b_name = l.b_name 
    AND u.vb_name = l.vb_name 
    AND u.timestamp = l.max_timestamp ), 
  ubereats_parsed AS ( 
  SELECT 
    b_name, 
    vb_name, 
    ue_slug, 
    JSON_EXTRACT_SCALAR(regular_hour, '$.startTime') AS start_time, 
    JSON_EXTRACT_SCALAR(regular_hour, '$.endTime') AS end_time, 
    JSON_EXTRACT_ARRAY(regular_hour, '$.daysBitArray') AS days_array 
  FROM 
    ubereats_raw, 
    UNNEST([JSON_EXTRACT(menus_string, '$.sections[0]')]) AS first_section, 
    UNNEST(JSON_EXTRACT_ARRAY(first_section, '$.regularHours')) AS regular_hour ), 
  ubereats_by_day AS ( 
  SELECT 
    ue_slug, 
    b_name, 
    vb_name, 
    CASE day_index 
      WHEN 0 THEN 'MONDAY' 
      WHEN 1 THEN 'TUESDAY' 
      WHEN 2 THEN 'WEDNESDAY' 
      WHEN 3 THEN 'THURSDAY' 
WHEN 4 THEN 'FRIDAY' 
WHEN 5 THEN 'SATURDAY' 
WHEN 6 THEN 'SUNDAY' 
END 
AS day_of_week, 
start_time, 
end_time, 
CAST(SPLIT(start_time, ':')[ 
OFFSET 
(0)] AS INT64) * 60 + CAST(SPLIT(start_time, ':')[ 
OFFSET 
(1)] AS INT64) AS start_minutes, 
CAST(SPLIT(end_time, ':')[ 
OFFSET 
(0)] AS INT64) * 60 + CAST(SPLIT(end_time, ':')[ 
OFFSET 
(1)] AS INT64) AS end_minutes 
FROM 
ubereats_parsed, 
UNNEST(GENERATE_ARRAY(0, 6)) AS day_index 
WHERE 
JSON_VALUE(days_array[SAFE_OFFSET(day_index)]) = 'true' ), 
latest_grubhub AS ( 
SELECT 
b_name, 
vb_name, 
MAX(timestamp) AS max_timestamp 
FROM 
`arboreal-vision-339901.take_home_v2.virtual_kitchen_grubhub_hours` 
GROUP BY 
b_name, 
vb_name ), 
grubhub_parsed AS ( 
SELECT 
g.slug AS gh_slug, 
g.b_name, 
g.vb_name, 
schedule_day AS day_of_week, 
JSON_EXTRACT_SCALAR(rule, '$.from') AS start_time, 
JSON_EXTRACT_SCALAR(rule, '$.to') AS end_time 
FROM 
`arboreal-vision-339901.take_home_v2.virtual_kitchen_grubhub_hours` g 
JOIN 
latest_grubhub l 
ON 
g.b_name = l.b_name 
AND g.vb_name = l.vb_name 
AND g.timestamp = l.max_timestamp, 
UNNEST(JSON_EXTRACT_ARRAY(g.response, 
'$.availability_by_catalog.STANDARD_DELIVERY.custom_schedules.schedule_rules')) AS 
rule, 
UNNEST(SPLIT(REPLACE(REPLACE(UPPER(JSON_EXTRACT_SCALAR(rule, '$.days_of_week')), 
'[', ''), ']', ''), ',')) AS schedule_day 
UNION ALL 
SELECT 
g.slug AS gh_slug, 
g.b_name, 
g.vb_name, 
CASE EXTRACT(DAYOFWEEK 
FROM 
CURRENT_DATE()) 
WHEN 1 THEN 'SUNDAY' 
WHEN 2 THEN 'MONDAY' 
WHEN 3 THEN 'TUESDAY' 
WHEN 4 THEN 'WEDNESDAY' 
WHEN 5 THEN 'THURSDAY' 
WHEN 6 THEN 'FRIDAY' 
WHEN 7 THEN 'SATURDAY' 
END 
AS day_of_week, 
JSON_VALUE(g.response, 
'$.today_availability_by_catalog.STANDARD_DELIVERY[0].from') AS start_time, 
JSON_VALUE(g.response, '$.today_availability_by_catalog.STANDARD_DELIVERY[0].to') 
AS end_time 
FROM 
`arboreal-vision-339901.take_home_v2.virtual_kitchen_grubhub_hours` g 
JOIN 
latest_grubhub l 
ON 
g.b_name = l.b_name 
AND g.vb_name = l.vb_name 
AND g.timestamp = l.max_timestamp 
WHERE 
JSON_EXTRACT(g.response, '$.today_availability_by_catalog.STANDARD_DELIVERY') IS 
NOT NULL 
AND JSON_EXTRACT(g.response, 
'$.availability_by_catalog.STANDARD_DELIVERY.custom_schedules.schedule_rules') IS NULL 
), 
grubhub_by_day AS ( 
SELECT 
gh_slug, 
b_name, 
vb_name, 
day_of_week, 
start_time, 
end_time, 
CAST(SPLIT(start_time, ':')[ 
OFFSET 
(0)] AS INT64) * 60 + CAST(SPLIT(start_time, ':')[ 
OFFSET 
(1)] AS INT64) AS start_minutes, 
CAST(SPLIT(end_time, ':')[ 
OFFSET 
(0)] AS INT64) * 60 + CAST(SPLIT(end_time, ':')[ 
OFFSET 
(1)] AS INT64) AS end_minutes 
FROM 
grubhub_parsed 
WHERE 
start_time IS NOT NULL 
AND end_time IS NOT NULL 
AND day_of_week IS NOT NULL ) 
SELECT 
g.gh_slug AS grubhub_slug, 
CONCAT(g.day_of_week, ': ', SUBSTR(g.start_time, 0, 5), '-', SUBSTR(g.end_time, 0, 
5)) AS grubhub_business_hours, 
u.ue_slug AS ubereats_slug, 
CONCAT(u.day_of_week, ': ', SUBSTR(u.start_time, 0, 5), '-', SUBSTR(u.end_time, 0, 
5)) AS ubereats_business_hours, 
CASE 
WHEN g.start_minutes >= u.start_minutes AND g.end_minutes <= u.end_minutes THEN 
'In Range' 
WHEN (ABS(g.start_minutes - u.start_minutes) <= 5 
AND g.end_minutes <= u.end_minutes) 
OR (g.start_minutes >= u.start_minutes 
AND ABS(g.end_minutes - u.end_minutes) <= 5) 
OR (ABS(g.start_minutes - u.start_minutes) <= 5 
AND ABS(g.end_minutes - u.end_minutes) <= 5) THEN 'Out of Range with 5 mins 
difference' 
ELSE 'Out of Range' 
END 
AS is_out_of_range 
FROM 
ubereats_by_day u 
JOIN 
grubhub_by_day g 
ON 
u.b_name = g.b_name 
AND u.vb_name = g.vb_name 
AND u.day_of_week = g.day_of_week 
ORDER BY 
g.b_name, 
g.vb_name, 
g.day_of_week; 