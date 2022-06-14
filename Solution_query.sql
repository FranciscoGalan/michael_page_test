/* Adding columns with requested values in tables 1 and 2 */
ALTER TABLE "table_1"
ADD COLUMN provider VARCHAR,
ADD COLUMN network VARCHAR
/* I'll drop column ad_name, since it has no values */ 
DROP COLUMN ad_name;

UPDATE "table_1"
SET provider = 'Platform 1',
	network = 'channel 1'
WHERE date IS NOT NULL;

ALTER TABLE "table_2"
ADD COLUMN provider VARCHAR,
ADD COLUMN network VARCHAR;

UPDATE "table_2"
SET provider = 'Platform 2',
	network = 'channel 2'
WHERE date IS NOT NULL;

/* The following query performs these steps:
 * 
 * 1. It cleans each table individually. Some tables, such as table 1, 2, and 5, had duplicate rows, that is,
 * they had the same ad displayed on the same day. I assumed that the add was shown at mutiples time during the day, which
 * 'date' column did not show. In those cases, I simply added the metrics for the day. Moreover, Table 4 had duplicate rows,
 * so I just removed them.
 * 
 * 2. It joins Tables 1 and 3, and Tables 2 and 4.
 * 
 * 3. It makes a union between Tables 1, 2, 3, and 4.
 * 
 * 4. It joins Tables 1-4 with Table 5. 
 * 
 * 5. It displays the requested variables. 
 *    */
WITH tables_1_to_4 AS
	(WITH tables_1_and_3 AS
		(WITH table_3_clean AS
			(WITH last_headlines AS 
				(SELECT t3.ad_id, headline1, headline2, headline3
				FROM table_3 AS t3 
				LEFT JOIN (
					SELECT ad_id, MAX(date) AS ad_last_date
					FROM table_3
					GROUP BY ad_id) AS last_headlines ON t3.ad_id = last_headlines.ad_id
				WHERE ad_last_date = date),
			campaign_dates AS 
				(SELECT campaign_id, MIN(date) AS campaign_start_date, MAX(date) AS campaign_end_date
				FROM table_3 AS t3
				GROUP BY campaign_id)
			SELECT date, account_id, t3.campaign_id, campaign_start_date, campaign_end_date, adset_id, t3.ad_id, last_headlines.headline1, last_headlines.headline2, last_headlines.headline3, final_url, path1, path2
			FROM table_3 AS t3
			LEFT JOIN last_headlines ON t3.ad_id = last_headlines.ad_id
			LEFT JOIN campaign_dates ON t3.campaign_id = campaign_dates.campaign_id),
		table_1_clean AS 
			(SELECT date, account_id, campaign_id, campaign_name, adset_id, adset_name, ad_id, ad_type, device, provider, network, SUM(spend) AS spend, SUM(clicks) AS clicks, SUM(imps) AS imps, SUM(conversions) AS conversions
			FROM table_1 AS t1
			GROUP BY date, account_id, campaign_id, campaign_name, adset_id, adset_name, ad_id, ad_type, device, provider, network
			ORDER BY date, account_id)
		SELECT t1.date, t1.account_id, t1.campaign_id, campaign_start_date, campaign_end_date, t1.campaign_name,  REPLACE(SPLIT_PART(t1.campaign_name, '|', 2), '_BR', '') AS campaign_name_short, t1.adset_id, adset_name, t1.ad_id, ad_type, device, spend, clicks, imps, conversions, provider, network, CONCAT(headline1, ' | ', headline2, ' | ', headline3) AS ad_name, final_url, path1, path2 
		FROM table_1_clean AS t1
		LEFT JOIN table_3_clean AS t3c 
			ON t1.date = t3c.date
			AND t1.account_id = t3c.account_id
			AND t1.campaign_id = t3c.campaign_id
			AND t1.adset_id = t3c.adset_id
			AND t1.ad_id = t3c.ad_id
		ORDER BY date, account_id, ad_id, device),
	tables_2_and_4 AS 
		(WITH table_2_clean AS
			(WITH campaign_dates_t2 AS 
				(SELECT campaign_id, MIN(date) AS campaign_start_date, MAX(date) AS campaign_end_date
				FROM table_2 AS t2
				GROUP BY campaign_id)
			SELECT date, account_id, t2.campaign_id, campaign_name, campaign_start_date, campaign_end_date, adset_id, adset_name, ad_id, ad_type, device, provider, network, SUM(spend) AS spend, SUM(clicks) AS clicks, SUM(imps) AS imps, SUM(conversions) AS conversions
			FROM table_2 AS t2
			LEFT JOIN campaign_dates_t2 ON t2.campaign_id = campaign_dates_t2.campaign_id
			GROUP BY date, account_id, t2.campaign_id, campaign_name, campaign_start_date, campaign_end_date, adset_id, adset_name, ad_id, ad_type, device, provider, network
			ORDER BY date, account_id),
		table_4_clean AS 
			(SELECT DISTINCT account_id, campaign_id, adset_id, ad_id, headline1, headline2, destination_url
			FROM table_4 AS t4)
		SELECT t2.date, t2.account_id, t2.campaign_id, campaign_start_date, campaign_end_date, t2.campaign_name, REPLACE(SPLIT_PART(campaign_name, '|', 2), '_BR', '') AS campaign_name_short, t2.adset_id, adset_name, t2.ad_id, ad_type, device, spend, clicks, imps, conversions, provider, network, CONCAT(headline1, ' | ', headline2) AS ad_name, destination_url AS final_url, NULL AS path1, NULL AS path2
		FROM table_2_clean AS t2
		LEFT JOIN table_4_clean AS t4
			ON t2.account_id = t4.account_id
			AND t2.campaign_id = t4.campaign_id
			AND t2.adset_id = t4.adset_id
			AND t2.ad_id = t4.ad_id
		ORDER BY date, t2.account_id, t2.ad_id, device)
	SELECT *
	FROM tables_1_and_3
	UNION
	SELECT *
	FROM tables_2_and_4),
table_5_clean AS
	(SELECT date, utm_source, campaign, SUM(sessions) AS sessions, SUM(users) AS users, SUM(new_users) AS new_users, SUM(page_views) AS page_views
	FROM table_5 AS t5
	GROUP BY date, utm_source, campaign
	ORDER BY date, utm_source, campaign)
SELECT 	t1_4.date, 
		provider, 
		network, 
		account_id,
		campaign_name_short,
		campaign_start_date,
		campaign_end_date,
		REPLACE(SPLIT_PART(campaign_name, '|', 3), '_FF', '') AS brand,
		SPLIT_PART(campaign_name, '|', 4) AS free_field,
		adset_name,
		TRIM(SPLIT_PART(adset_name, '|', 1)) AS adset_group,
		CONCAT(SPLIT_PART(final_url, '/', 3), CASE WHEN path1 IS NULL THEN '' WHEN path1 = '' THEN '' ELSE '/' END, path1, CASE WHEN path2 IS NULL THEN '' WHEN path2 = '' THEN '' ELSE '/' END, path2) AS display_path,
		ad_name,
		ad_type,
		device,
		spend,
		clicks,
		imps AS impressions,
		conversions,
		sessions,
		users,
		new_users,
		page_views
FROM tables_1_to_4 AS t1_4
LEFT JOIN table_5_clean AS t5
	ON t1_4.date = t5.date
	AND t1_4.provider = t5.utm_source
	AND t1_4.campaign_name_short = UPPER(t5.campaign)
;









