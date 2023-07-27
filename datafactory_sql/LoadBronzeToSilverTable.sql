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
        st.end_date = GETDATE(),
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
        st.end_date = bsp.ingestUTC
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
        bsp.source_table_name,
        bsp.review_id,
        bsp.user_id,
        bsp.business_id,
        bsp.stars,
        bsp.useful,
        bsp.funny,
        bsp.cool,
        bsp.[text],
        bsp.[date],
        bsp.[name],
        bsp.[address],
        bsp.city,
        bsp.[state],
        bsp.postal_code,
        bsp.latitude,
        bsp.longitude,
        bsp.business_stars,
        bsp.review_count,
        bsp.is_open,
        bsp.attributes,
        bsp.categories,
        bsp.[hours],
        bsp.checkin_date,
        bsp.compliment_count,
        bsp.yelping_since,
        bsp.elite,
        bsp.friends,
        bsp.fans,
        bsp.average_stars,
        bsp.compliment_hot,
        bsp.compliment_more,
        bsp.compliment_profile,
        bsp.compliment_cute,
        bsp.compliment_list,
        bsp.compliment_note,
        bsp.compliment_plain,
        bsp.compliment_cool,
        bsp.compliment_funny,
        bsp.compliment_writer,
        bsp.compliment_photos,
        bsp.ingestUTC,
        bsp.CHECKSUM_key,
        [start_date] = bsp.ingestUTC,
        end_date = NULL,
        is_current = 1
    INTO #NewSilverTableRows
    FROM 
        delta_silver_table st
        RIGHT JOIN #BronzeSilverPrep bsp
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