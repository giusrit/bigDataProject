#delete and create db
hive -e "drop database if exists itwiki cascade; create database itwiki";

echo "DATABASE CREATED"

hdfs dfs -rmr /home/hdfs/itwiki

# Import and convert of page
# Get only page with 2 <= id <= 277087
sqoop import --connect jdbc:mysql://localhost/itwiki --username root --password hadoop --query "select page_id,page_namespace,convert(page_title using utf8) as page_title,convert(page_restrictions using utf8) as page_restrictions, page_counter, page_is_redirect,page_is_new,page_random,convert(page_touched using utf8) as page_touched,convert(page_links_updated using utf8) as page_links_updated,page_latest,page_len,page_no_title_convert,convert(page_content_model using utf8) as page_content_model,convert(page_lang using utf8) as page_lang from page where \$CONDITIONS and page_id >= 2 and page_id <= 277087" --split-by page_id --target-dir /home/hdfs/itwiki/page --driver com.mysql.jdbc.Driver --hive-import --hive-overwrite --hive-table itwiki.page --create-hive-table

echo "PAGES IMPORTED"

# Import and convert of change_tag
# Get only change_tag ?????????????????????
sqoop import --connect jdbc:mysql://localhost/itwiki --username root --password hadoop --query "select ct_id, ct_rc_id, ct_log_id, ct_rev_id, convert(ct_tag using utf8) as ct_tag,convert(ct_params using utf8) as ct_params from change_tag where \$CONDITIONS" --split-by ct_id --target-dir /home/hdfs/itwiki/change_tag --driver com.mysql.jdbc.Driver --hive-import --hive-overwrite --hive-table itwiki.change_tag --create-hive-table -m 512

echo "CHANGE_TAG IMPORTED"
