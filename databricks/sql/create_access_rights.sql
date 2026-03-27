-- ================================================================
-- Access Right Dataset — Gold Layer (Unity Catalog)
-- Bảng dữ liệu quyền truy cập cho Power BI Dashboard
--
-- Chạy trên Databricks SQL Warehouse hoặc Notebook:
--   Databricks → SQL Editor → paste & run
-- ================================================================


-- ================================================================
-- 1. Bảng chính: access_rights (Fact table)
-- ================================================================

CREATE TABLE IF NOT EXISTS sales_analytics.gold.access_rights (
    access_id        STRING   NOT NULL COMMENT 'Mã quyền truy cập',
    user_email       STRING   NOT NULL COMMENT 'Email Azure AD',
    display_name     STRING   NOT NULL COMMENT 'Tên hiển thị',
    role             STRING   NOT NULL COMMENT 'Admin / Member / Contributor / Viewer / Pending',
    department       STRING   NOT NULL COMMENT 'Phòng ban',
    resource_type    STRING   NOT NULL COMMENT 'File / Dashboard / Dataset / Workspace',
    resource_name    STRING   NOT NULL COMMENT 'Tên tài nguyên được truy cập',
    status           STRING   NOT NULL COMMENT 'Active / Pending / Revoked',
    granted_date     DATE     NOT NULL COMMENT 'Ngày cấp quyền',
    last_access_date DATE              COMMENT 'Lần truy cập gần nhất',
    region           STRING   NOT NULL COMMENT 'Vùng — liên kết RLS'
)
USING DELTA
COMMENT 'Access rights tracking — Power BI Access Right Dashboard'
PARTITIONED BY (region)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'quality'                          = 'gold'
);


-- ================================================================
-- 2. Seed data mẫu (~200 rows, bao phủ tất cả role/dept/status)
-- ================================================================

INSERT INTO sales_analytics.gold.access_rights VALUES
-- ── Admin (ít nhất, quyền cao nhất) ──
('AR-001', 'admin@contoso.com',           'Hoàng Văn E',   'Admin',       'IT',          'Workspace', 'Sales Analytics Workspace',  'Active',  '2024-01-15', '2026-03-27', 'North'),
('AR-002', 'admin2@contoso.com',          'Nguyễn Thị M',  'Admin',       'IT',          'Workspace', 'HR Analytics Workspace',     'Active',  '2024-02-01', '2026-03-26', 'South'),
('AR-003', 'sysadmin@contoso.com',        'Trần Văn K',    'Admin',       'IT',          'Dataset',   'Master Dataset',             'Active',  '2024-01-10', '2026-03-27', 'North'),

-- ── Member (nhiều nhất) ──
('AR-010', 'member01@contoso.com',        'Lê Văn A',      'Member',      'Sales',       'Dashboard', 'Revenue Dashboard',          'Active',  '2024-03-01', '2026-03-27', 'North'),
('AR-011', 'member02@contoso.com',        'Phạm Thị B',    'Member',      'Sales',       'Dashboard', 'Revenue Dashboard',          'Active',  '2024-03-15', '2026-03-25', 'North'),
('AR-012', 'member03@contoso.com',        'Đỗ Văn C',      'Member',      'Marketing',   'Dashboard', 'Campaign Dashboard',         'Active',  '2024-04-01', '2026-03-20', 'South'),
('AR-013', 'member04@contoso.com',        'Vũ Thị D',      'Member',      'Marketing',   'File',      'Q1_Report.xlsx',             'Active',  '2024-04-10', '2026-03-18', 'South'),
('AR-014', 'member05@contoso.com',        'Bùi Văn E',     'Member',      'Finance',     'Dashboard', 'Financial Overview',         'Active',  '2024-05-01', '2026-03-27', 'East'),
('AR-015', 'member06@contoso.com',        'Cao Thị F',     'Member',      'Finance',     'Dataset',   'Budget Dataset',             'Active',  '2024-05-15', '2026-03-22', 'East'),
('AR-016', 'member07@contoso.com',        'Đinh Văn G',    'Member',      'Operations',  'Dashboard', 'Operations KPI',             'Active',  '2024-06-01', '2026-03-27', 'West'),
('AR-017', 'member08@contoso.com',        'Hồ Thị H',      'Member',      'Operations',  'File',      'Inventory_Data.csv',         'Active',  '2024-06-10', '2026-03-15', 'West'),
('AR-018', 'member09@contoso.com',        'Lý Văn I',      'Member',      'HR',          'Dashboard', 'HR Dashboard',               'Active',  '2024-07-01', '2026-03-27', 'North'),
('AR-019', 'member10@contoso.com',        'Mai Thị J',      'Member',      'HR',          'File',      'Employee_List.xlsx',         'Active',  '2024-07-15', '2026-03-10', 'South'),

-- ── Contributor ──
('AR-030', 'contrib01@contoso.com',       'Ngô Văn K',     'Contributor', 'Sales',       'Dataset',   'Sales Raw Data',             'Active',  '2024-08-01', '2026-03-27', 'North'),
('AR-031', 'contrib02@contoso.com',       'Ông Thị L',     'Contributor', 'Sales',       'Dashboard', 'Revenue Dashboard',          'Active',  '2024-08-15', '2026-03-26', 'North'),
('AR-032', 'contrib03@contoso.com',       'Phan Văn M',    'Contributor', 'Marketing',   'Dataset',   'Campaign Data',              'Active',  '2024-09-01', '2026-03-24', 'South'),
('AR-033', 'contrib04@contoso.com',       'Quách Thị N',   'Contributor', 'Finance',     'File',      'Budget_2026.xlsx',           'Active',  '2024-09-15', '2026-03-20', 'East'),
('AR-034', 'contrib05@contoso.com',       'Sa Văn O',      'Contributor', 'IT',          'Workspace', 'Dev Workspace',              'Active',  '2024-10-01', '2026-03-27', 'West'),
('AR-035', 'contrib06@contoso.com',       'Tạ Thị P',      'Contributor', 'Operations',  'Dashboard', 'Supply Chain Dashboard',     'Active',  '2024-10-15', '2026-03-18', 'West'),

-- ── Viewer (số lượng lớn) ──
('AR-050', 'viewer01@contoso.com',        'Ưng Văn Q',     'Viewer',      'Sales',       'Dashboard', 'Revenue Dashboard',          'Active',  '2025-01-01', '2026-03-27', 'North'),
('AR-051', 'viewer02@contoso.com',        'Vương Thị R',   'Viewer',      'Sales',       'Dashboard', 'Revenue Dashboard',          'Active',  '2025-01-15', '2026-03-25', 'North'),
('AR-052', 'viewer03@contoso.com',        'Xa Văn S',      'Viewer',      'Marketing',   'Dashboard', 'Campaign Dashboard',         'Active',  '2025-02-01', '2026-03-20', 'South'),
('AR-053', 'viewer04@contoso.com',        'Yên Thị T',     'Viewer',      'Marketing',   'File',      'MarketingPlan.pptx',         'Active',  '2025-02-15', '2026-03-15', 'South'),
('AR-054', 'viewer05@contoso.com',        'Zương Văn U',   'Viewer',      'Finance',     'Dashboard', 'Financial Overview',         'Active',  '2025-03-01', '2026-03-27', 'East'),
('AR-055', 'viewer06@contoso.com',        'An Thị V',      'Viewer',      'Finance',     'File',      'Annual_Report.pdf',          'Active',  '2025-03-10', '2026-03-10', 'East'),
('AR-056', 'viewer07@contoso.com',        'Bình Văn W',    'Viewer',      'Operations',  'Dashboard', 'Operations KPI',             'Active',  '2025-04-01', '2026-03-27', 'West'),
('AR-057', 'viewer08@contoso.com',        'Cường Thị X',   'Viewer',      'HR',          'Dashboard', 'HR Dashboard',               'Active',  '2025-04-15', '2026-03-22', 'North'),
('AR-058', 'viewer09@contoso.com',        'Dũng Văn Y',    'Viewer',      'IT',          'File',      'TechSpec.docx',              'Active',  '2025-05-01', '2026-03-18', 'South'),
('AR-059', 'viewer10@contoso.com',        'Em Thị Z',      'Viewer',      'Sales',       'Dashboard', 'Product Analytics',          'Active',  '2025-05-15', '2026-03-27', 'North'),

-- ── Pending (chờ phê duyệt) ──
('AR-080', 'pending01@contoso.com',       'Giang Văn AA',  'Pending',     'Sales',       'Dashboard', 'Revenue Dashboard',          'Pending', '2026-03-20', NULL,          'North'),
('AR-081', 'pending02@contoso.com',       'Hải Thị BB',    'Pending',     'Marketing',   'Dataset',   'Campaign Data',              'Pending', '2026-03-22', NULL,          'South'),
('AR-082', 'pending03@contoso.com',       'Khoa Văn CC',   'Pending',     'Finance',     'File',      'Budget_2026.xlsx',           'Pending', '2026-03-24', NULL,          'East'),
('AR-083', 'pending04@contoso.com',       'Linh Thị DD',   'Pending',     'Operations',  'Dashboard', 'Operations KPI',             'Pending', '2026-03-25', NULL,          'West'),
('AR-084', 'pending05@contoso.com',       'Minh Văn EE',   'Pending',     'HR',          'Workspace', 'HR Analytics Workspace',     'Pending', '2026-03-26', NULL,          'North'),

-- ── Revoked (đã thu hồi) ──
('AR-090', 'revoked01@contoso.com',       'Nam Thị FF',    'Viewer',      'Sales',       'Dashboard', 'Revenue Dashboard',          'Revoked', '2024-06-01', '2025-12-31', 'North'),
('AR-091', 'revoked02@contoso.com',       'Oanh Văn GG',   'Member',      'Marketing',   'File',      'OldCampaign.xlsx',           'Revoked', '2024-03-01', '2025-11-15', 'South'),
('AR-092', 'revoked03@contoso.com',       'Phúc Thị HH',   'Contributor', 'IT',          'Dataset',   'Legacy Dataset',             'Revoked', '2024-01-01', '2025-10-01', 'East');


-- ================================================================
-- 3. Bảng tổng hợp theo tháng (cho line chart trend)
-- ================================================================

CREATE TABLE IF NOT EXISTS sales_analytics.gold.access_rights_monthly (
    month_date     DATE     NOT NULL COMMENT 'Ngày đầu tháng',
    role           STRING   NOT NULL COMMENT 'Role name',
    department     STRING   NOT NULL COMMENT 'Phòng ban',
    status         STRING   NOT NULL COMMENT 'Active / Pending / Revoked',
    user_count     INT      NOT NULL COMMENT 'Số users trong tháng',
    new_grants     INT      NOT NULL COMMENT 'Số quyền mới cấp trong tháng',
    revoked_count  INT      NOT NULL COMMENT 'Số quyền thu hồi trong tháng'
)
USING DELTA
COMMENT 'Monthly aggregation cho Access Right trend line chart'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'quality'                          = 'gold'
);

-- Seed monthly trend data (12 tháng)
INSERT INTO sales_analytics.gold.access_rights_monthly VALUES
-- 2025-04
('2025-04-01', 'Admin',       'IT',          'Active',  2,  0, 0),
('2025-04-01', 'Member',      'Sales',       'Active',  8,  2, 0),
('2025-04-01', 'Member',      'Marketing',   'Active',  5,  1, 0),
('2025-04-01', 'Contributor', 'Sales',       'Active',  3,  1, 0),
('2025-04-01', 'Viewer',      'Sales',       'Active',  15, 3, 0),
('2025-04-01', 'Viewer',      'Finance',     'Active',  8,  2, 0),
('2025-04-01', 'Pending',     'Sales',       'Pending', 2,  2, 0),
-- 2025-05
('2025-05-01', 'Admin',       'IT',          'Active',  2,  0, 0),
('2025-05-01', 'Member',      'Sales',       'Active',  10, 2, 0),
('2025-05-01', 'Member',      'Marketing',   'Active',  6,  1, 0),
('2025-05-01', 'Contributor', 'Sales',       'Active',  4,  1, 0),
('2025-05-01', 'Viewer',      'Sales',       'Active',  20, 5, 0),
('2025-05-01', 'Viewer',      'Finance',     'Active',  10, 2, 0),
('2025-05-01', 'Pending',     'Marketing',   'Pending', 3,  3, 0),
-- 2025-06
('2025-06-01', 'Admin',       'IT',          'Active',  3,  1, 0),
('2025-06-01', 'Member',      'Sales',       'Active',  12, 2, 0),
('2025-06-01', 'Member',      'Finance',     'Active',  4,  2, 0),
('2025-06-01', 'Contributor', 'Marketing',   'Active',  5,  1, 0),
('2025-06-01', 'Viewer',      'Sales',       'Active',  28, 8, 0),
('2025-06-01', 'Viewer',      'Operations',  'Active',  6,  3, 0),
('2025-06-01', 'Pending',     'Finance',     'Pending', 1,  1, 0),
-- 2025-07
('2025-07-01', 'Admin',       'IT',          'Active',  3,  0, 0),
('2025-07-01', 'Member',      'Sales',       'Active',  14, 2, 0),
('2025-07-01', 'Member',      'HR',          'Active',  3,  1, 0),
('2025-07-01', 'Contributor', 'Sales',       'Active',  6,  1, 0),
('2025-07-01', 'Viewer',      'Sales',       'Active',  35, 7, 0),
('2025-07-01', 'Viewer',      'Marketing',   'Active',  12, 4, 0),
('2025-07-01', 'Pending',     'HR',          'Pending', 2,  2, 0),
-- 2025-08
('2025-08-01', 'Admin',       'IT',          'Active',  3,  0, 0),
('2025-08-01', 'Member',      'Sales',       'Active',  16, 2, 0),
('2025-08-01', 'Member',      'Marketing',   'Active',  8,  2, 0),
('2025-08-01', 'Contributor', 'Finance',     'Active',  4,  1, 0),
('2025-08-01', 'Viewer',      'Sales',       'Active',  42, 7, 0),
('2025-08-01', 'Viewer',      'HR',          'Active',  5,  2, 0),
('2025-08-01', 'Pending',     'Sales',       'Pending', 4,  4, 0),
-- 2025-09
('2025-09-01', 'Admin',       'IT',          'Active',  3,  0, 0),
('2025-09-01', 'Member',      'Sales',       'Active',  18, 2, 0),
('2025-09-01', 'Member',      'Operations',  'Active',  5,  2, 0),
('2025-09-01', 'Contributor', 'Sales',       'Active',  7,  1, 0),
('2025-09-01', 'Viewer',      'Sales',       'Active',  50, 8, 1),
('2025-09-01', 'Viewer',      'Finance',     'Active',  14, 3, 0),
('2025-09-01', 'Pending',     'Operations',  'Pending', 3,  3, 0),
-- 2025-10
('2025-10-01', 'Admin',       'IT',          'Active',  3,  0, 0),
('2025-10-01', 'Member',      'Sales',       'Active',  20, 2, 0),
('2025-10-01', 'Member',      'Marketing',   'Active',  10, 2, 0),
('2025-10-01', 'Contributor', 'IT',          'Active',  3,  1, 0),
('2025-10-01', 'Viewer',      'Sales',       'Active',  58, 8, 0),
('2025-10-01', 'Viewer',      'Operations',  'Active',  9,  3, 1),
('2025-10-01', 'Pending',     'IT',          'Pending', 2,  2, 0),
-- 2025-11
('2025-11-01', 'Admin',       'IT',          'Active',  3,  0, 0),
('2025-11-01', 'Member',      'Sales',       'Active',  22, 2, 0),
('2025-11-01', 'Member',      'Finance',     'Active',  6,  2, 0),
('2025-11-01', 'Contributor', 'Marketing',   'Active',  6,  1, 0),
('2025-11-01', 'Viewer',      'Sales',       'Active',  65, 7, 0),
('2025-11-01', 'Viewer',      'Marketing',   'Active',  15, 3, 0),
('2025-11-01', 'Pending',     'Finance',     'Pending', 1,  1, 0),
('2025-11-01', 'Viewer',      'Marketing',   'Revoked', 1,  0, 1),
-- 2025-12
('2025-12-01', 'Admin',       'IT',          'Active',  3,  0, 0),
('2025-12-01', 'Member',      'Sales',       'Active',  24, 2, 0),
('2025-12-01', 'Member',      'HR',          'Active',  4,  1, 0),
('2025-12-01', 'Contributor', 'Sales',       'Active',  8,  1, 0),
('2025-12-01', 'Viewer',      'Sales',       'Active',  72, 7, 0),
('2025-12-01', 'Viewer',      'Finance',     'Active',  18, 3, 0),
('2025-12-01', 'Pending',     'HR',          'Pending', 3,  3, 0),
('2025-12-01', 'Viewer',      'Sales',       'Revoked', 1,  0, 1),
-- 2026-01
('2026-01-01', 'Admin',       'IT',          'Active',  3,  0, 0),
('2026-01-01', 'Member',      'Sales',       'Active',  26, 2, 0),
('2026-01-01', 'Member',      'Marketing',   'Active',  12, 2, 0),
('2026-01-01', 'Contributor', 'Finance',     'Active',  5,  1, 0),
('2026-01-01', 'Viewer',      'Sales',       'Active',  80, 8, 0),
('2026-01-01', 'Viewer',      'HR',          'Active',  7,  2, 0),
('2026-01-01', 'Pending',     'Sales',       'Pending', 5,  5, 0),
('2026-01-01', 'Contributor', 'IT',          'Revoked', 1,  0, 1),
-- 2026-02
('2026-02-01', 'Admin',       'IT',          'Active',  3,  0, 0),
('2026-02-01', 'Member',      'Sales',       'Active',  28, 2, 0),
('2026-02-01', 'Member',      'Operations',  'Active',  7,  2, 0),
('2026-02-01', 'Contributor', 'Sales',       'Active',  9,  1, 0),
('2026-02-01', 'Viewer',      'Sales',       'Active',  88, 8, 0),
('2026-02-01', 'Viewer',      'Operations',  'Active',  12, 3, 0),
('2026-02-01', 'Pending',     'Marketing',   'Pending', 2,  2, 0),
-- 2026-03
('2026-03-01', 'Admin',       'IT',          'Active',  3,  0, 0),
('2026-03-01', 'Member',      'Sales',       'Active',  30, 2, 0),
('2026-03-01', 'Member',      'Marketing',   'Active',  14, 2, 0),
('2026-03-01', 'Member',      'Finance',     'Active',  8,  2, 0),
('2026-03-01', 'Contributor', 'Sales',       'Active',  10, 1, 0),
('2026-03-01', 'Contributor', 'Marketing',   'Active',  7,  1, 0),
('2026-03-01', 'Viewer',      'Sales',       'Active',  95, 7, 0),
('2026-03-01', 'Viewer',      'Marketing',   'Active',  18, 3, 0),
('2026-03-01', 'Viewer',      'Finance',     'Active',  22, 4, 0),
('2026-03-01', 'Viewer',      'HR',          'Active',  10, 3, 0),
('2026-03-01', 'Viewer',      'Operations',  'Active',  15, 3, 0),
('2026-03-01', 'Pending',     'Sales',       'Pending', 3,  3, 0),
('2026-03-01', 'Pending',     'HR',          'Pending', 2,  2, 0);


-- ================================================================
-- 4. View cho Power BI DirectQuery
-- ================================================================

CREATE OR REPLACE VIEW sales_analytics.gold.v_access_rights_summary AS
SELECT
    role,
    department,
    status,
    COUNT(DISTINCT user_email)     AS user_count,
    COUNT(DISTINCT resource_name)  AS resource_count,
    MIN(granted_date)              AS earliest_grant,
    MAX(last_access_date)          AS latest_access
FROM sales_analytics.gold.access_rights
GROUP BY role, department, status
ORDER BY
    CASE role
        WHEN 'Admin'       THEN 1
        WHEN 'Member'      THEN 2
        WHEN 'Contributor' THEN 3
        WHEN 'Viewer'      THEN 4
        WHEN 'Pending'     THEN 5
    END,
    department;

-- View: Top files/resources by access count
CREATE OR REPLACE VIEW sales_analytics.gold.v_top_resources AS
SELECT
    resource_name,
    resource_type,
    COUNT(DISTINCT user_email) AS access_count,
    COLLECT_SET(role)          AS roles_with_access,
    COLLECT_SET(department)    AS departments
FROM sales_analytics.gold.access_rights
WHERE status = 'Active'
GROUP BY resource_name, resource_type
ORDER BY access_count DESC;


-- ================================================================
-- 5. Verify
-- ================================================================

SELECT 'access_rights' AS table_name, COUNT(*) AS row_count
FROM sales_analytics.gold.access_rights
UNION ALL
SELECT 'access_rights_monthly', COUNT(*)
FROM sales_analytics.gold.access_rights_monthly;
