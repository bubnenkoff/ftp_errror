BEGIN;

-- CREATE TABLE "xml_files" ------------------------------------
CREATE TABLE "public"."xml_files" ( 
	"id" BigInt DEFAULT nextval('xml_files_id_seq'::regclass) NOT NULL,
	"file_name" Character Varying( 150 ) NOT NULL,
	"file_type" Character Varying( 150 ),
	"region" Character Varying( 150 ) NOT NULL,
	"arch_name" Character Varying( 150 ) NOT NULL,
	"forced_processing" Boolean,
	"section_name" Character Varying( 150 ) NOT NULL,
	"parsing_status" Character Varying( 150 ),
	"xml_date" Timestamp Without Time Zone NOT NULL,
	"processed_date" Timestamp Without Time Zone,
	"insert_status" Character Varying( 2044 ),
	"purchaseNumber" Character Varying( 2044 ),
	"docPublishDate" Timestamp With Time Zone,
	"is_processing" Boolean,
	"fz" Character Varying( 50 ),
	"jobNumber" Integer,
	"action_type" Character Varying,
	"exists_status" Character Varying,
	"region_id" BigInt,
	CONSTRAINT "unique_file_name" UNIQUE( "file_name" ) );
 ;
-- -------------------------------------------------------------

-- CREATE INDEX "idx_xml_files_2" ------------------------------
CREATE INDEX "idx_xml_files_2" ON "public"."xml_files" USING btree( "purchaseNumber" Asc NULLS Last, "docPublishDate" Asc NULLS Last );
-- -------------------------------------------------------------

-- CREATE INDEX "idx_xml_files_3" ------------------------------
CREATE INDEX "idx_xml_files_3" ON "public"."xml_files" USING btree( "section_name" Asc NULLS Last, "jobNumber" Asc NULLS Last );
-- -------------------------------------------------------------

-- CREATE INDEX "index_id" -------------------------------------
CREATE INDEX "index_id" ON "public"."xml_files" USING btree( "id" Asc NULLS Last );
-- -------------------------------------------------------------

-- CREATE INDEX "test_index_three_fields" ----------------------
CREATE INDEX "test_index_three_fields" ON "public"."xml_files" USING btree( "section_name" Asc NULLS Last, "region" Asc NULLS Last, "processed_date" Asc NULLS Last, "insert_status" Asc NULLS Last );
-- -------------------------------------------------------------

COMMIT;
