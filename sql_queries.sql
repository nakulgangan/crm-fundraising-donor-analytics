-- ============================================================
-- PROJECT 2: CRM Fundraising & Donor Analytics
-- ThankQ-Style Data Pipeline — Rainbows Hospice Simulation
-- Author: Nakul Gangan
-- ============================================================

-- ============================================================
-- SCHEMA
-- ============================================================

CREATE TABLE donors (
    donor_id        VARCHAR(10) PRIMARY KEY,
    donor_type      VARCHAR(20),   -- 'Individual', 'Corporate', 'Trust/Foundation'
    acquisition_channel VARCHAR(30),-- 'Event', 'Direct Mail', 'Online', 'Referral', 'Legacy'
    region          VARCHAR(30),
    first_gift_date DATE,
    is_active       BOOLEAN
);

CREATE TABLE donations (
    donation_id     VARCHAR(15) PRIMARY KEY,
    donor_id        VARCHAR(10) REFERENCES donors(donor_id),
    amount          NUMERIC(10,2),
    donation_date   DATE,
    campaign        VARCHAR(40),   -- 'Rainbow Run', 'Christmas Appeal', 'Legacy', 'Corporate'
    fund_type       VARCHAR(20),   -- 'Restricted', 'Unrestricted'
    gift_aid        BOOLEAN
);

CREATE TABLE campaigns (
    campaign_id     VARCHAR(15) PRIMARY KEY,
    campaign_name   VARCHAR(40),
    start_date      DATE,
    end_date        DATE,
    target_amount   NUMERIC(10,2),
    channel         VARCHAR(20)
);

-- ============================================================
-- QUERY 1: De-duplication — Identify Duplicate Donor Records
-- Common CRM data quality issue in ThankQ/similar systems
-- ============================================================
WITH ranked AS (
    SELECT
        donor_id,
        region,
        acquisition_channel,
        first_gift_date,
        ROW_NUMBER() OVER (
            PARTITION BY region, acquisition_channel, first_gift_date
            ORDER BY donor_id
        ) AS rn
    FROM donors
)
SELECT
    donor_id,
    region,
    acquisition_channel,
    first_gift_date,
    rn AS duplicate_rank
FROM ranked
WHERE rn > 1
ORDER BY first_gift_date;

-- ============================================================
-- QUERY 2: Donor Retention & Lapse Analysis
-- Identifies lapsed donors (no gift in 12+ months)
-- ============================================================
WITH last_gift AS (
    SELECT
        donor_id,
        MAX(donation_date)  AS last_gift_date,
        SUM(amount)         AS lifetime_value,
        COUNT(donation_id)  AS total_gifts
    FROM donations
    GROUP BY donor_id
)
SELECT
    d.donor_id,
    d.donor_type,
    d.acquisition_channel,
    d.region,
    lg.last_gift_date,
    lg.lifetime_value,
    lg.total_gifts,
    DATE_PART('day', CURRENT_DATE - lg.last_gift_date) AS days_since_last_gift,
    CASE
        WHEN lg.last_gift_date >= CURRENT_DATE - INTERVAL '12 months' THEN 'Active'
        WHEN lg.last_gift_date >= CURRENT_DATE - INTERVAL '24 months' THEN 'Lapsed (1-2yr)'
        ELSE 'Lapsed (2yr+)'
    END AS donor_status
FROM donors d
JOIN last_gift lg ON d.donor_id = lg.donor_id
ORDER BY lg.last_gift_date ASC;

-- ============================================================
-- QUERY 3: Campaign Performance — Actual vs Target
-- With Gift Aid uplift calculation (25p per £1 donated)
-- ============================================================
WITH campaign_totals AS (
    SELECT
        campaign,
        COUNT(DISTINCT donor_id)                      AS unique_donors,
        SUM(amount)                                   AS gross_income,
        SUM(CASE WHEN gift_aid THEN amount * 0.25 ELSE 0 END) AS gift_aid_uplift,
        COUNT(donation_id)                            AS total_donations,
        ROUND(AVG(amount), 2)                         AS avg_donation
    FROM donations
    GROUP BY campaign
)
SELECT
    ct.campaign,
    ct.unique_donors,
    ct.gross_income,
    ct.gift_aid_uplift,
    ct.gross_income + ct.gift_aid_uplift              AS total_income_incl_gift_aid,
    ct.avg_donation,
    c.target_amount,
    ROUND((ct.gross_income / NULLIF(c.target_amount,0)) * 100, 1) AS pct_target_achieved,
    CASE
        WHEN ct.gross_income >= c.target_amount         THEN 'Exceeded'
        WHEN ct.gross_income >= c.target_amount * 0.85  THEN 'On Track'
        ELSE 'Below Target'
    END AS status
FROM campaign_totals ct
LEFT JOIN campaigns c ON ct.campaign = c.campaign_name
ORDER BY ct.gross_income DESC;

-- ============================================================
-- QUERY 4: High-Value Donor Segmentation (RFM-style)
-- Recency, Frequency, Monetary for targeted outreach
-- ============================================================
WITH rfm AS (
    SELECT
        d.donor_id,
        d.donor_type,
        d.acquisition_channel,
        MAX(dn.donation_date)                          AS last_gift,
        COUNT(dn.donation_id)                          AS frequency,
        SUM(dn.amount)                                 AS monetary,
        DATE_PART('day', CURRENT_DATE - MAX(dn.donation_date)) AS recency_days
    FROM donors d
    JOIN donations dn ON d.donor_id = dn.donor_id
    GROUP BY d.donor_id, d.donor_type, d.acquisition_channel
)
SELECT
    donor_id,
    donor_type,
    acquisition_channel,
    recency_days,
    frequency,
    ROUND(monetary, 2)  AS lifetime_value,
    CASE
        WHEN monetary >= 5000 AND recency_days <= 365 AND frequency >= 3 THEN 'Major Donor'
        WHEN monetary >= 1000 AND recency_days <= 365                   THEN 'Mid-Level'
        WHEN recency_days <= 365                                         THEN 'Regular'
        ELSE 'Lapsed'
    END AS segment
FROM rfm
ORDER BY monetary DESC;

-- ============================================================
-- QUERY 5: UK GDPR Compliance Check
-- Flags donors without valid consent records
-- ============================================================
SELECT
    d.donor_id,
    d.donor_type,
    d.region,
    CASE WHEN d.is_active IS NULL THEN 'Missing consent flag' ELSE 'OK' END AS consent_status,
    COUNT(dn.donation_id) AS donations_on_record
FROM donors d
LEFT JOIN donations dn ON d.donor_id = dn.donor_id
WHERE d.is_active IS NULL
GROUP BY d.donor_id, d.donor_type, d.region, d.is_active
ORDER BY donations_on_record DESC;
