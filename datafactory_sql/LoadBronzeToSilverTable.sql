-- Non-tested Load Bronze To Silver with SCD Type 2
CREATE PROCEDURE dbo.LoadBronzeToSilverTable
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRANSACTION;


    SELECT 
        source_table_name,
        review_id,
        user_id,
        business_id,
        stars,
        useful,
        funny,
        cool,
        [text],
        [date],
        [name],
        [address],
        city,
        [state],
        postal_code,
        latitude,
        longitude,
        business_stars,
        review_count,
        is_open,
        attributes,
        categories,
        [hours],
        checkin_date,
        compliment_count,
        yelping_since,
        elite,
        friends,
        fans,
        average_stars,
        compliment_hot,
        compliment_more,
        compliment_profile,
        compliment_cute,
        compliment_list,
        compliment_note,
        compliment_plain,
        compliment_cool,
        compliment_funny,
        compliment_writer,
        compliment_photos,
        ingestUTC,
        CHECKSUM_key = CASE source_table_name
            -- Load from yelp_business
            WHEN 'business' THEN CHECKSUM(
                [name],
                [address],
                city,
                [state],
                postal_code,
                latitude,
                longitude,
                business_id,
                business_stars,
                review_count,
                is_open,
                attributes,
                categories,
                [hours]
            )
            -- load from yelp_checkin
            WHEN 'checkin' THEN CHECKSUM(
                business_id,
                checkin_date
            )
            -- load from yelp_tip
            WHEN 'tip' THEN CHECKSUM(
                source_table_name,
                [user_id],
                business_id,
                [text],
                [date],
                compliment_count
            )
            -- load from yelp_review
            WHEN 'review' THEN CHECKSUM(
                source_table_name,
                review_id,
                [user_id],
                business_id,
                stars,
                useful,
                funny,
                cool,
                [text],
                [date]
            )
            -- load from yelp_user
            WHEN 'user' THEN CHECKSUM(
                source_table_name,
                [user_id],
                [name],
                review_count,
                yelping_since,
                useful,
                funny,
                cool,
                elite,
                friends,
                fans,
                average_stars,
                compliment_hot,
                compliment_more,
                compliment_profile,
                compliment_cute,
                compliment_list,
                compliment_note,
                compliment_plain,
                compliment_cool,
                compliment_funny,
                compliment_writer,
                compliment_photos
            )
        END
    INTO #BronzeSilverPrep
    FROM delta_bronze_table

    -- Deleted (Missing Rows)
    UPDATE st
    SET 
        st.end_date = bsp.utc,
        st.is_current = 0
    FROM 
        delta_silver_table st
        LEFT JOIN #BronzeSilverPrep bsp
        on 
            st.is_current = 1
            AND CONCAT(
                st.source_table_name,
                st.review_id,
                st.user_id,
                st.business_id,
                st.checksum_key
            ) = CONCAT(
                bsp.source_table_name,
                bsp.review_id,
                bsp.user_id,
                bsp.business_id,
                bsp.checksum_key
            )
            AND bsp.checksum_key = null


    -- Matching Rows
    UPDATE st
    SET
        st.end_date = bsp.utc
    FROM 
        delta_silver_table st
        LEFT JOIN #BronzeSilverPrep bsp
        on 
            st.is_current = 1
            AND CONCAT(
                st.source_table_name,
                st.review_id,
                st.user_id,
                st.business_id,
                st.checksum_key
            ) = CONCAT(
                bsp.source_table_name,
                bsp.review_id,
                bsp.user_id,
                bsp.business_id,
                bsp.checksum_key
            )
            AND bsp.checksum_key <> null

    -- New Rows
    SELECT
        st.source_table_name,
        st.review_id,
        st.user_id,
        st.business_id,
        st.stars,
        st.useful,
        st.funny,
        st.cool,
        st.[text],
        st.[date],
        st.[name],
        st.[address],
        st.city,
        st.[state],
        st.postal_code,
        st.latitude,
        st.longitude,
        st.business_stars,
        st.review_count,
        st.is_open,
        st.attributes,
        st.categories,
        st.[hours],
        st.checkin_date,
        st.compliment_count,
        st.yelping_since,
        st.elite,
        st.friends,
        st.fans,
        st.average_stars,
        st.compliment_hot,
        st.compliment_more,
        st.compliment_profile,
        st.compliment_cute,
        st.compliment_list,
        st.compliment_note,
        st.compliment_plain,
        st.compliment_cool,
        st.compliment_funny,
        st.compliment_writer,
        st.compliment_photos,
        st.ingestUTC,
        st.CHECKSUM_key,
        [start_date] = st.ingestUTC,
        end_date = NULL,
        is_current = 1
    INTO #NewSilverTableRows
    FROM 
        delta_silver_table st
        LEFT JOIN #BronzeSilverPrep bsp
        on 
            st.is_current = 1
            AND CONCAT(
                st.source_table_name,
                st.review_id,
                st.user_id,
                st.business_id,
                st.checksum_key
            ) = CONCAT(
                bsp.source_table_name,
                bsp.review_id,
                bsp.user_id,
                bsp.business_id,
                bsp.checksum_key
            )
            AND st.checksum_key = null
    -- Load
    INSERT INTO delta_silver_table(
        source_table_name,
        review_id,
        user_id,
        business_id,
        stars,
        useful,
        funny,
        cool,
        [text],
        [date],
        [name],
        [address],
        city,
        [state],
        postal_code,
        latitude,
        longitude,
        business_stars,
        review_count,
        is_open,
        attributes,
        categories,
        [hours],
        checkin_date,
        compliment_count,
        yelping_since,
        elite,
        friends,
        fans,
        average_stars,
        compliment_hot,
        compliment_more,
        compliment_profile,
        compliment_cute,
        compliment_list,
        compliment_note,
        compliment_plain,
        compliment_cool,
        compliment_funny,
        compliment_writer,
        compliment_photos,
        ingestUTC,
        CHECKSUM_key,
        [start_date],
        end_date,
        is_current
    )
    SELECT
        source_table_name,
        review_id,
        user_id,
        business_id,
        stars,
        useful,
        funny,
        cool,
        [text],
        [date],
        [name],
        [address],
        city,
        [state],
        postal_code,
        latitude,
        longitude,
        business_stars,
        review_count,
        is_open,
        attributes,
        categories,
        [hours],
        checkin_date,
        compliment_count,
        yelping_since,
        elite,
        friends,
        fans,
        average_stars,
        compliment_hot,
        compliment_more,
        compliment_profile,
        compliment_cute,
        compliment_list,
        compliment_note,
        compliment_plain,
        compliment_cool,
        compliment_funny,
        compliment_writer,
        compliment_photos,
        ingestUTC,
        CHECKSUM_key,
        [start_date],
        end_date,
        is_current
    FROM
        #NewSilverTableRows
    
    COMMIT TRANSACTION;
END;