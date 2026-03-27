-- ================================================================
-- Security Mapping Table — Gold Layer (Unity Catalog)
-- Dùng cho Power BI Row-Level Security (Dynamic RLS)
--
-- Chạy trên Databricks SQL Warehouse hoặc Notebook:
--   Databricks → SQL Editor → paste & run
-- ================================================================

-- 1. Tạo bảng SecurityMapping
CREATE TABLE IF NOT EXISTS sales_analytics.gold.security_mapping (
    user_email       STRING   NOT NULL COMMENT 'UPN — Azure AD email',
    display_name     STRING   NOT NULL COMMENT 'Tên hiển thị',
    role             STRING   NOT NULL COMMENT 'Manager / Analyst / Director / Admin',
    allowed_region   STRING   NOT NULL COMMENT 'Region được phép xem dữ liệu'
)
USING DELTA
COMMENT 'Row-Level Security mapping: user → allowed regions. Import vào Power BI.'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'quality'                          = 'gold'
);

-- 2. Seed data mẫu (thay email thực tế của tổ chức)
INSERT INTO sales_analytics.gold.security_mapping VALUES
    -- Managers: mỗi người quản lý 1 vùng
    ('manager_north@contoso.com', 'Nguyễn Văn A',  'Manager',  'North'),
    ('manager_south@contoso.com', 'Trần Thị B',    'Manager',  'South'),
    ('manager_east@contoso.com',  'Lê Văn C',      'Manager',  'East'),
    ('manager_west@contoso.com',  'Phạm Thị D',    'Manager',  'West'),

    -- Director: thấy tất cả regions (1 row per region)
    ('director@contoso.com',      'Hoàng Văn E',   'Director', 'North'),
    ('director@contoso.com',      'Hoàng Văn E',   'Director', 'South'),
    ('director@contoso.com',      'Hoàng Văn E',   'Director', 'East'),
    ('director@contoso.com',      'Hoàng Văn E',   'Director', 'West'),

    -- Analysts: thấy 1-2 vùng tùy phân công
    ('analyst_01@contoso.com',    'Đỗ Văn F',      'Analyst',  'North'),
    ('analyst_01@contoso.com',    'Đỗ Văn F',      'Analyst',  'South'),
    ('analyst_02@contoso.com',    'Vũ Thị G',      'Analyst',  'East'),
    ('analyst_02@contoso.com',    'Vũ Thị G',      'Analyst',  'West');

-- 3. Verify
SELECT
    role,
    display_name,
    COLLECT_SET(allowed_region) AS regions
FROM sales_analytics.gold.security_mapping
GROUP BY role, display_name
ORDER BY role, display_name;
