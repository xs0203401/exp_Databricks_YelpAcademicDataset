CREATE PROCEDURE sp_truncateStgTables
AS
BEGIN

    TRUNCATE TABLE stg_yelp_checkin;
    TRUNCATE TABLE stg_yelp_tip;
    TRUNCATE TABLE stg_yelp_review;
    TRUNCATE TABLE stg_yelp_business;
    TRUNCATE TABLE stg_yelp_user;

    TRUNCATE TABLE [dbo].[delta_bronze_table];

END