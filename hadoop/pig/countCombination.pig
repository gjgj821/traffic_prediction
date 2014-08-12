-- count Combinations
register './combine_udf.py' using jython as combineUDFS;
data_raw = LOAD '/user/jiangshen/dsp_log_for_hbase/2014-08-01/joined' USING PigStorage('|') AS(
       -- Bid Request
       adx                                   :int,          --U
       request_id                            :chararray,
       type                                  :int,
       adSlot_id                             :int,
       adSolt_creative_type                  :int,
       adSlot_allowed_size                   :chararray,
       adSlot_disallowed_ad_category_id      :int,
       adSlot_fixed_price                    :float,
       adSlot_min_cpm_micros                 :float,        -- reserve price
       user_id                               :chararray,
       user_Data_id                          :chararray,
       user_Data_segment                     :chararray,
       device_id                             :int,
       device_supplemental_id                :int,
       device_os                             :chararray,    --U
       device_os_version                     :chararray,    --U
       device_brand                          :chararray,    --u
       device_model                          :chararray,    --u
       device_device_type                    :int,          --u
       device_screen_orientation             :chararray,
       device_screen_size                    :chararray,
       device_device_pixel_ratio_millis      :chararray,
       detworkConnection_connection_type     :int,          --U
       detworkConnection_carrier_id          :int,          --U
       ip                                    :chararray,
       location_latitude                     :chararray,
       location_longitude                    :chararray,
       location_altitude                     :int,
       location_country_id                   :int,          --U
       location_region_id                    :int,          --U
       location_city_id                      :int,          --U
       location_postal_code                  :chararray,
       location_lac                          :chararray,
       app_id                                :int,
       app_name                              :chararray,
       app_bundle                            :chararray,
       app_itunes_id                         :int,
       app_version                           :chararray,
       app_paid                              :int,
       app_in_app_purchase                   :chararray,
       app_category_id                       :int,          --U
       is_test                               :int,
       timestamp                             :int,          -- time base
       location_geo_criteria_id              :int,
       device_user_agent                     :chararray,
       app_limei_app_id                      :int,          --U
       device_model_id                       :int,
       -- Wangwei add
       hyperlocal_coners                     :chararray,
       hyperlocal_center_point               :chararray,
       adslot_excluded_attribute             :chararray,
       -- BidResponse
       res_request_id                        :chararray,
       res_adslot_id                         :int,
       res_cpm_micros                        :float,
       res_campaign_id                       :int,
       res_adgroup_id                        :int,
       res_creative_id                       :int,
       res_size                              :chararray,
       res_bid_strategy                      :int,          --U
       res_creative_type                     :int,
       res_company_id                        :int,
       res_timestamp                         :int,
       res_targeting_tag_ids                 :chararray,    -- type?
       res_template_parameter                :chararray,    -- type?
       res_ab_test_ids                       :chararray,    -- type?
       res_adx                               :int,
       imp_request_id                        :int,
       imp_adslot_id                         :int,
       imp_closing_price                     :float,        -- dealing price
       imp_ip                                :chararray,
       imp_timestamp                         :int,
       clk_request_id                        :int,
       clk_adslot_id                         :int,
       clk_ip                                :chararray,
       clk_timestamp                         :int,
       conv_request_id                       :int,
       conv_adslot_id                        :int,
       conv_adx                              :int,
       conv_device_id                        :int,
       conv_app_id                           :int,
       conv_ip                               :chararray,
       conv_timestamp                        :int,
       conv_adgroup_id                       :int,
       conv_creative_id                      :int
       );

-- test purpose
data_raw = SAMPLE data_raw 0.0001;

data_need = FOREACH data_raw GENERATE combineUDFS.combine_udf(
    adx,
    device_os_version,
    device_brand,
    device_model,
    device_device_type,
    detworkConnection_connection_type,
    detworkConnection_carrier_id,
    location_country_id,
    location_region_id,
    location_city_id,
    app_category_id,
    app_limei_app_id
)
AS combine_b;
--DUMP data_need;

data_fla = FOREACH data_need GENERATE FLATTEN(combine_b) AS comb;
grpd = GROUP data_fla BY comb PARALLEL 10;
--DUMP grpd;
uniqcnt = FOREACH grpd GENERATE group, COUNT(data_fla);
--DUMP uniqcnt;
STORE uniqcnt INTO '/tmp/wangwei/data/piece' USING PigStorage('|');
