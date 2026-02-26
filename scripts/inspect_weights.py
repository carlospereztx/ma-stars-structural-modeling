import duckdb

con = duckdb.connect("db/ma_stars.duckdb")

print("\nSample rows:")
print(con.execute("""
SELECT *
FROM contract_county_weights
LIMIT 10
""").df())

print("\nOne contract-year weight distribution:")
print(con.execute("""
SELECT contract_id, year, county_fips, enrollment, w_enroll
FROM contract_county_weights
WHERE contract_id = (
    SELECT contract_id FROM contract_county_weights LIMIT 1
)
AND year = (
    SELECT year FROM contract_county_weights LIMIT 1
)
ORDER BY w_enroll DESC
LIMIT 10
""").df())

print("\nDistinct counties per year:")
print(con.execute("""
SELECT year,
       COUNT(DISTINCT county_fips) AS unique_counties
FROM contract_county_weights
GROUP BY 1
ORDER BY 1
""").df())