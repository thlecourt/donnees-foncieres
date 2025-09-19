----- Ce script extrait les parcelles avec au moins un propriétaire public dans un nouveau schéma, avec une structure similaire aux Fichiers Fonciers et harmonisée entre millésimes de 2009 à 2021


------------------------------------------
----CREATION DES STRUCTURES DE TABLE------
------------------------------------------


---2009 (24s)---

DO
$do$
DECLARE
	millesime text;
	millesime_short text;
	ccodep text;
  	partition_tablename text;
  	a_ccodep text[];
	table_temp_dep text;
	schema_name text;
	table_name_parcelle text;
	table_name_new text;
BEGIN
	millesime = '2009';
	millesime_short = RIGHT(millesime,2);
	table_temp_dep = 'dep'||millesime;
	schema_name = 'ff_'||millesime;
	table_name_parcelle = 'fftp_'||millesime||'_pnb10_parcelle';
	table_name_new = 'public'||millesime_short;
	
	EXECUTE format(
		$$
		--Création de la table des parcelles
		DROP TABLE IF EXISTS nat.%I CASCADE;
		CREATE TABLE IF NOT EXISTS nat.%I(
			idpar character varying(14) COLLATE pg_catalog."default",
			idsec character varying(10) COLLATE pg_catalog."default",
			idprocpte character varying(11) COLLATE pg_catalog."default",
			idparref character varying(14) COLLATE pg_catalog."default",
			idsecref character varying(10) COLLATE pg_catalog."default",
			idvoie character varying(9) COLLATE pg_catalog."default",
			idcom character varying(5) COLLATE pg_catalog."default",
			idcomtxt character varying(45) COLLATE pg_catalog."default",
			ccodep character varying(2) COLLATE pg_catalog."default",
			ccodir character varying(1) COLLATE pg_catalog."default",
			ccocom character varying(3) COLLATE pg_catalog."default",
			ccopre character varying(3) COLLATE pg_catalog."default",
			ccosec character varying(2) COLLATE pg_catalog."default",
			dnupla character varying(4) COLLATE pg_catalog."default",
			dcntpa integer,
			dsrpar character varying(1) COLLATE pg_catalog."default",
			dnupro character varying(6) COLLATE pg_catalog."default",
			jdatat character varying(8) COLLATE pg_catalog."default",
			dreflf character varying(5) COLLATE pg_catalog."default",
			gpdl character varying(1) COLLATE pg_catalog."default",
			cprsecr character varying(3) COLLATE pg_catalog."default",
			ccosecr character varying(2) COLLATE pg_catalog."default",
			dnuplar character varying(4) COLLATE pg_catalog."default",
			dnupdl character varying(3) COLLATE pg_catalog."default",
			gurbpa character varying(1) COLLATE pg_catalog."default",
			dparpi character varying(4) COLLATE pg_catalog."default",
			ccoarp character varying(1) COLLATE pg_catalog."default",
			gparnf character varying(1) COLLATE pg_catalog."default",
			gparbat character varying(1) COLLATE pg_catalog."default",
			dnvoiri character varying(4) COLLATE pg_catalog."default",
			dindic character varying(1) COLLATE pg_catalog."default",
			ccovoi character varying(5) COLLATE pg_catalog."default",
			ccoriv character varying(4) COLLATE pg_catalog."default",
			ccocif character varying(4) COLLATE pg_catalog."default",
			jdatatan integer,
			jannatmin integer,
			jannatmax integer,
			janbilmin integer,
			nsuf integer,
			ssuf integer,
			cgrnumd character varying(2) COLLATE pg_catalog."default",
			cgrnumdtxt character varying(19) COLLATE pg_catalog."default",
			dcntsfd integer,
			dcntsol integer,
			dcntagri integer,
			dcntbois integer,
			dcntnat integer,
			dcnteau integer,
			dcnt01 integer,
			dcnt02 integer,
			dcnt03 integer,
			dcnt04 integer,
			dcnt06 integer,
			dcnt07 integer,
			dcnt09 integer,
			dcnt10 integer,
			dcnt11 integer,
			dcnt12 integer,
			dcnt13 integer,
			schemrem integer,
			nlocal integer,
			nlocmaison integer,
			nlocappt integer,
			nlochabit integer,
			nloccom integer,
			nloccomrdc integer,
			nlocdep integer,
			tlocdomin character varying(11) COLLATE pg_catalog."default",
			nbat integer,
			npevaffh integer,
			npevph integer,
			stoth integer,
			smoyh integer,
			npiecemoy double precision,
			stotdsueic integer,
			nvacant integer,
			nmediocre integer,
			nloghlm integer,
			noccprop integer,
			nocclocat integer,
			typoocc character varying(16) COLLATE pg_catalog."default",
			npevp integer,
			stotp integer,
			smoyp integer,
			npevd integer,
			stotd integer,
			smoyd integer,
			spevtot integer,
			tpevdom_n character varying(13) COLLATE pg_catalog."default",
			tpevdom_s character varying(14) COLLATE pg_catalog."default",
			nlot integer,
			cmp integer,
			ndroit integer,
			ndroitindi integer,
			ndroitpro integer,
			ndroitges integer,
			typprop character varying(52) COLLATE pg_catalog."default",
			typproppro character varying(52) COLLATE pg_catalog."default",
			typpropges character varying(52) COLLATE pg_catalog."default",
			geomloc geometry(MultiPoint,2154),
			geompar geometry(MultiPolygon,2154),
			source_geo character varying(34) COLLATE pg_catalog."default",
			vecteur character varying(1) COLLATE pg_catalog."default",
			idpk integer,
			--SUITE A CONSERVER
			a_prop_cod_prop bpchar[] COLLATE pg_catalog."default",
			a_prop_id bigint[],
			a_prop_idu text[] COLLATE pg_catalog."default",
			a_prop_public boolean[],
			a_prop_cominterco boolean[],
			a_prop_com_niv character varying[] COLLATE pg_catalog."default",
			a_prop_com_niv_libelle text[] COLLATE pg_catalog."default",
			a_ges_cod_prop bpchar[] COLLATE pg_catalog."default",
			a_ges_id bigint[],
			a_ges_idu text[] COLLATE pg_catalog."default",
			a_ges_public boolean[],
			a_ges_cominterco boolean[],
			a_ges_com_niv character varying[] COLLATE pg_catalog."default",
			a_ges_com_niv_libelle text[] COLLATE pg_catalog."default",
			cominterco boolean,
			com_niv int,
			idcom22 character varying(5) COLLATE pg_catalog."default",
			PRIMARY KEY(ccodep,idpk),
			UNIQUE(ccodep,idpar)
		)
		PARTITION BY LIST(ccodep);
		
		--CREATION D'UNE TABLE TEMPORAIRE DES DEPARTEMENTS
		CREATE SCHEMA IF NOT EXISTS temp;
		DROP TABLE IF EXISTS temp.dep;
		CREATE TABLE temp.dep AS (
			WITH sub AS (
				SELECT par.ccodep
				FROM %I.%I par
				GROUP BY par.ccodep)
			SELECT ARRAY_AGG(ccodep) ccodep
			FROM sub
		);		
		$$, table_name_new, table_name_new, schema_name, table_name_parcelle);

		--Création des partitions par département
		a_ccodep = (SELECT dep.ccodep FROM temp.dep dep);

		FOREACH ccodep IN ARRAY ARRAY[a_ccodep]
		LOOP
			partition_tablename = table_name_new||'_'||ccodep;
		EXECUTE format(
		$$ 
				CREATE TABLE IF NOT EXISTS nat.%I
				PARTITION OF nat.%I
				FOR VALUES IN (%L)
			$$, partition_tablename,table_name_new,ccodep);
		END LOOP;
		
		EXECUTE format(
		$$
			DROP TABLE temp.dep
		$$);
		
END
$do$



----2011 (36s)---

DO
$do$
DECLARE
	millesime text;
	millesime_short text;
	ccodep text;
  	partition_tablename text;
  	a_ccodep text[];
	table_temp_dep text;
	schema_name text;
	table_name_parcelle text;
	table_name_new text;
BEGIN
	millesime = '2011';
	millesime_short = RIGHT(millesime,2);
	table_temp_dep = 'dep'||millesime;
	schema_name = 'ff_'||millesime;
	table_name_parcelle = 'fftp_'||millesime||'_pnb10_parcelle';
	table_name_new = 'public'||millesime_short;
	
	EXECUTE format(
		$$
		--Création de la table des parcelles
		DROP TABLE IF EXISTS nat.%I CASCADE;
		CREATE TABLE IF NOT EXISTS nat.%I(
			idpar character varying(14) COLLATE pg_catalog."default",
			idsec character varying(10) COLLATE pg_catalog."default",
			idprocpte character varying(11) COLLATE pg_catalog."default",
			idparref character varying(14) COLLATE pg_catalog."default",
			idsecref character varying(10) COLLATE pg_catalog."default",
			idvoie character varying(9) COLLATE pg_catalog."default",
			idcom character varying(5) COLLATE pg_catalog."default",
			idcomtxt character varying(45) COLLATE pg_catalog."default",
			ccodep character varying(2) COLLATE pg_catalog."default",
			ccodir character varying(1) COLLATE pg_catalog."default",
			ccocom character varying(3) COLLATE pg_catalog."default",
			ccopre character varying(3) COLLATE pg_catalog."default",
			ccosec character varying(2) COLLATE pg_catalog."default",
			dnupla character varying(4) COLLATE pg_catalog."default",
			dcntpa integer,
			dsrpar character varying(1) COLLATE pg_catalog."default",
			dnupro character varying(6) COLLATE pg_catalog."default",
			jdatat character varying(8) COLLATE pg_catalog."default",
			dreflf character varying(5) COLLATE pg_catalog."default",
			gpdl character varying(1) COLLATE pg_catalog."default",
			cprsecr character varying(3) COLLATE pg_catalog."default",
			ccosecr character varying(2) COLLATE pg_catalog."default",
			dnuplar character varying(4) COLLATE pg_catalog."default",
			dnupdl character varying(3) COLLATE pg_catalog."default",
			gurbpa character varying(1) COLLATE pg_catalog."default",
			dparpi character varying(4) COLLATE pg_catalog."default",
			ccoarp character varying(1) COLLATE pg_catalog."default",
			gparnf character varying(1) COLLATE pg_catalog."default",
			gparbat character varying(1) COLLATE pg_catalog."default",
			dnvoiri character varying(4) COLLATE pg_catalog."default",
			dindic character varying(1) COLLATE pg_catalog."default",
			ccovoi character varying(5) COLLATE pg_catalog."default",
			ccoriv character varying(4) COLLATE pg_catalog."default",
			ccocif character varying(4) COLLATE pg_catalog."default",
			jdatatan integer,
			jannatmin integer,
			jannatmax integer,
			janbilmin integer,
			nsuf integer,
			ssuf integer,
			cgrnumd character varying(2) COLLATE pg_catalog."default",
			cgrnumdtxt character varying(19) COLLATE pg_catalog."default",
			dcntsfd integer,
			dcntsol integer,
			dcntagri integer,
			dcntbois integer,
			dcntnat integer,
			dcnteau integer,
			dcnt01 integer,
			dcnt02 integer,
			dcnt03 integer,
			dcnt04 integer,
			dcnt06 integer,
			dcnt07 integer,
			dcnt09 integer,
			dcnt10 integer,
			dcnt11 integer,
			dcnt12 integer,
			dcnt13 integer,
			schemrem integer,
			nlocal integer,
			nlocmaison integer,
			nlocappt integer,
			nlochabit integer,
			nloccom integer,
			nloccomrdc integer,
			nlocdep integer,
			nlocburx integer,
			tlocdomin character varying(11) COLLATE pg_catalog."default",
			nbat integer,
			npevaffh integer,
			npevph integer,
			stoth integer,
			smoyh integer,
			npiecemoy double precision,
			stotdsueic integer,
			nvacant integer,
			nmediocre integer,
			nloghlm integer,
			noccprop integer,
			nocclocat integer,
			typoocc character varying(13) COLLATE pg_catalog."default",
			npevp integer,
			stotp integer,
			smoyp integer,
			npevd integer,
			stotd integer,
			smoyd integer,
			spevtot integer,
			tpevdom_n character varying(13) COLLATE pg_catalog."default",
			tpevdom_s character varying(14) COLLATE pg_catalog."default",
			nlot integer,
			cmp integer,
			ndroit integer,
			ndroitindi integer,
			ndroitpro integer,
			ndroitges integer,
			typprop character varying(52) COLLATE pg_catalog."default",
			typproppro character varying(52) COLLATE pg_catalog."default",
			typpropges character varying(52) COLLATE pg_catalog."default",
			geomloc geometry(MultiPoint,2154),
			geompar geometry(MultiPolygon,2154),
			source_geo character varying(34) COLLATE pg_catalog."default",
			vecteur character varying(1) COLLATE pg_catalog."default",
			idpk integer NOT NULL,
			--SUITE A CONSERVER
			a_prop_cod_prop bpchar[] COLLATE pg_catalog."default",
			a_prop_id bigint[],
			a_prop_idu text[] COLLATE pg_catalog."default",
			a_prop_public boolean[],
			a_prop_cominterco boolean[],
			a_prop_com_niv character varying[] COLLATE pg_catalog."default",
			a_prop_com_niv_libelle text[] COLLATE pg_catalog."default",
			a_ges_cod_prop bpchar[] COLLATE pg_catalog."default",
			a_ges_id bigint[],
			a_ges_idu text[] COLLATE pg_catalog."default",
			a_ges_public boolean[],
			a_ges_cominterco boolean[],
			a_ges_com_niv character varying[] COLLATE pg_catalog."default",
			a_ges_com_niv_libelle text[] COLLATE pg_catalog."default",
			cominterco boolean,
			com_niv int,
			idcom22 character varying(5) COLLATE pg_catalog."default",
			PRIMARY KEY(ccodep,idpk),
			UNIQUE(ccodep,idpar)
		)
		PARTITION BY LIST(ccodep);
		
		--CREATION D'UNE TABLE TEMPORAIRE DES DEPARTEMENTS
		CREATE SCHEMA IF NOT EXISTS temp;
		DROP TABLE IF EXISTS temp.dep;
		CREATE TABLE temp.dep AS (
			WITH sub AS (
				SELECT par.ccodep
				FROM %I.%I par
				GROUP BY par.ccodep)
			SELECT ARRAY_AGG(ccodep) ccodep
			FROM sub
		);		
		$$, table_name_new, table_name_new, schema_name, table_name_parcelle);

		--Création des partitions par département
		a_ccodep = (SELECT dep.ccodep FROM temp.dep dep);

		FOREACH ccodep IN ARRAY ARRAY[a_ccodep]
		LOOP
			partition_tablename = table_name_new||'_'||ccodep;
		EXECUTE format(
		$$ 
				CREATE TABLE IF NOT EXISTS nat.%I
				PARTITION OF nat.%I
				FOR VALUES IN (%L)
			$$, partition_tablename,table_name_new,ccodep);
		END LOOP;
		
		EXECUTE format(
		$$
			DROP TABLE temp.dep
		$$);
		
END
$do$


---2012 (37s)---

DO
$do$
DECLARE
	millesime text;
	millesime_short text;
	ccodep text;
  	partition_tablename text;
  	a_ccodep text[];
	table_temp_dep text;
	schema_name text;
	table_name_parcelle text;
	table_name_new text;
BEGIN
	millesime = '2012';
	millesime_short = RIGHT(millesime,2);
	table_temp_dep = 'dep'||millesime;
	schema_name = 'ff_'||millesime;
	table_name_parcelle = 'fftp_'||millesime||'_pnb10_parcelle';
	table_name_new = 'public'||millesime_short;
	
	EXECUTE format(
		$$
		--Création de la table des parcelles
		DROP TABLE IF EXISTS nat.%I CASCADE;
		CREATE TABLE IF NOT EXISTS nat.%I(
			idpar character varying(14) COLLATE pg_catalog."default",
			idsec character varying(10) COLLATE pg_catalog."default",
			idprocpte character varying(11) COLLATE pg_catalog."default",
			idparref character varying(14) COLLATE pg_catalog."default",
			idsecref character varying(10) COLLATE pg_catalog."default",
			idvoie character varying(9) COLLATE pg_catalog."default",
			idcom character varying(5) COLLATE pg_catalog."default",
			idcomtxt character varying(45) COLLATE pg_catalog."default",
			ccodep character varying(2) COLLATE pg_catalog."default",
			ccodir character varying(1) COLLATE pg_catalog."default",
			ccocom character varying(3) COLLATE pg_catalog."default",
			ccopre character varying(3) COLLATE pg_catalog."default",
			ccosec character varying(2) COLLATE pg_catalog."default",
			dnupla character varying(4) COLLATE pg_catalog."default",
			dcntpa integer,
			dsrpar character varying(1) COLLATE pg_catalog."default",
			dnupro character varying(6) COLLATE pg_catalog."default",
			jdatat character varying(8) COLLATE pg_catalog."default",
			jdatatv character varying(8) COLLATE pg_catalog."default",
			dreflf character varying(5) COLLATE pg_catalog."default",
			gpdl character varying(1) COLLATE pg_catalog."default",
			cprsecr character varying(3) COLLATE pg_catalog."default",
			ccosecr character varying(2) COLLATE pg_catalog."default",
			dnuplar character varying(4) COLLATE pg_catalog."default",
			dnupdl character varying(3) COLLATE pg_catalog."default",
			gurbpa character varying(1) COLLATE pg_catalog."default",
			dparpi character varying(4) COLLATE pg_catalog."default",
			ccoarp character varying(1) COLLATE pg_catalog."default",
			gparnf character varying(1) COLLATE pg_catalog."default",
			gparbat character varying(1) COLLATE pg_catalog."default",
			dnvoiri character varying(4) COLLATE pg_catalog."default",
			dindic character varying(1) COLLATE pg_catalog."default",
			ccovoi character varying(5) COLLATE pg_catalog."default",
			ccoriv character varying(4) COLLATE pg_catalog."default",
			ccocif character varying(4) COLLATE pg_catalog."default",
			cconvo character varying(4) COLLATE pg_catalog."default",
			dvoilib character varying(26) COLLATE pg_catalog."default",
			idparm character varying(14) COLLATE pg_catalog."default",
			ccocomm integer,
			ccoprem character varying(3) COLLATE pg_catalog."default",
			ccosecm character varying(2) COLLATE pg_catalog."default",
			dnuplam character varying(4) COLLATE pg_catalog."default",
			type character varying(1) COLLATE pg_catalog."default",
			typetxt character varying(16) COLLATE pg_catalog."default",
			jdatatan integer,
			jannatmin integer,
			jannatmax integer,
			jannatminh integer,
			jannatmaxh integer,
			janbilmin integer,
			nsuf integer,
			ssuf integer,
			cgrnumd character varying(2) COLLATE pg_catalog."default",
			cgrnumdtxt character varying(19) COLLATE pg_catalog."default",
			dcntsfd integer,
			dcntsol integer,
			dcntagri integer,
			dcntbois integer,
			dcntnat integer,
			dcnteau integer,
			dcnt01 integer,
			dcnt02 integer,
			dcnt03 integer,
			dcnt04 integer,
			dcnt06 integer,
			dcnt07 integer,
			dcnt09 integer,
			dcnt10 integer,
			dcnt11 integer,
			dcnt12 integer,
			dcnt13 integer,
			schemrem integer,
			nlocal integer,
			nlocmaison integer,
			nlocappt integer,
			nlochabit integer,
			nloccom integer,
			nloccomrdc integer,
			nloccomter integer,
			nloccomsec integer,
			nlocdep integer,
			nlocburx integer,
			tlocdomin character varying(11) COLLATE pg_catalog."default",
			nbat integer,
			npevaffh integer,
			npevph integer,
			stoth integer,
			smoyh integer,
			npiecemoy double precision,
			stotdsueic integer,
			nhabvacant integer,
			nactvacant integer,
			nmediocre integer,
			nloghlm integer,
			noccprop integer,
			nocclocat integer,
			typoocc character varying(13) COLLATE pg_catalog."default",
			npevp integer,
			stotp integer,
			smoyp integer,
			npevd integer,
			stotd integer,
			smoyd integer,
			spevtot integer,
			tpevdom_n character varying(13) COLLATE pg_catalog."default",
			tpevdom_s character varying(14) COLLATE pg_catalog."default",
			nlot integer,
			pdlmp character varying(1) COLLATE pg_catalog."default",
			ctpdl character varying(5) COLLATE pg_catalog."default",
			ndroit integer,
			ndroitindi integer,
			ndroitpro integer,
			ndroitges integer,
			typprop character varying(7) COLLATE pg_catalog."default",
			typproptxt character varying(57) COLLATE pg_catalog."default",
			typproppro character varying(7) COLLATE pg_catalog."default",
			typpropprotxt character varying(57) COLLATE pg_catalog."default",
			typpropges character varying(7) COLLATE pg_catalog."default",
			typpropgestxt character varying(57) COLLATE pg_catalog."default",
			locprop character varying(1) COLLATE pg_catalog."default",
			locproptxt character varying(21) COLLATE pg_catalog."default",
			geomloc geometry(MultiPoint,2154),
			geompar geometry(MultiPolygon,2154),
			source_geo character varying(34) COLLATE pg_catalog."default",
			vecteur character varying(1) COLLATE pg_catalog."default",
			idpk integer NOT NULL,
			--SUITE A CONSERVER
			a_prop_cod_prop bpchar[] COLLATE pg_catalog."default",
			a_prop_id bigint[],
			a_prop_idu text[] COLLATE pg_catalog."default",
			a_prop_public boolean[],
			a_prop_cominterco boolean[],
			a_prop_com_niv character varying[] COLLATE pg_catalog."default",
			a_prop_com_niv_libelle text[] COLLATE pg_catalog."default",
			a_ges_cod_prop bpchar[] COLLATE pg_catalog."default",
			a_ges_id bigint[],
			a_ges_idu text[] COLLATE pg_catalog."default",
			a_ges_public boolean[],
			a_ges_cominterco boolean[],
			a_ges_com_niv character varying[] COLLATE pg_catalog."default",
			a_ges_com_niv_libelle text[] COLLATE pg_catalog."default",
			cominterco boolean,
			com_niv int,
			idcom22 character varying(5) COLLATE pg_catalog."default",
			PRIMARY KEY(ccodep,idpk),
			UNIQUE(ccodep,idpar)
		)
		PARTITION BY LIST(ccodep);
		
		--CREATION D'UNE TABLE TEMPORAIRE DES DEPARTEMENTS
		CREATE SCHEMA IF NOT EXISTS temp;
		DROP TABLE IF EXISTS temp.dep;
		CREATE TABLE temp.dep AS (
			WITH sub AS (
				SELECT par.ccodep
				FROM %I.%I par
				GROUP BY par.ccodep)
			SELECT ARRAY_AGG(ccodep) ccodep
			FROM sub
		);		
		$$, table_name_new, table_name_new, schema_name, table_name_parcelle);

		--Création des partitions par département
		a_ccodep = (SELECT dep.ccodep FROM temp.dep dep);

		FOREACH ccodep IN ARRAY ARRAY[a_ccodep]
		LOOP
			partition_tablename = table_name_new||'_'||ccodep;
		EXECUTE format(
		$$ 
				CREATE TABLE IF NOT EXISTS nat.%I
				PARTITION OF nat.%I
				FOR VALUES IN (%L)
			$$, partition_tablename,table_name_new,ccodep);
		END LOOP;
		
		EXECUTE format(
		$$
			DROP TABLE temp.dep
		$$);
		
END
$do$


---2013 (39s)---

DO
$do$
DECLARE
	millesime text;
	millesime_short text;
	ccodep text;
  	partition_tablename text;
  	a_ccodep text[];
	table_temp_dep text;
	schema_name text;
	table_name_parcelle text;
	table_name_new text;
BEGIN
	millesime = '2013';
	millesime_short = RIGHT(millesime,2);
	table_temp_dep = 'dep'||millesime;
	schema_name = 'ff_'||millesime;
	table_name_parcelle = 'fftp_'||millesime||'_pnb10_parcelle';
	table_name_new = 'public'||millesime_short;
	
	EXECUTE format(
		$$
		--Création de la table des parcelles
		DROP TABLE IF EXISTS nat.%I CASCADE;
		CREATE TABLE IF NOT EXISTS nat.%I(
			idpar character varying(14) COLLATE pg_catalog."default",
			idsec character varying(10) COLLATE pg_catalog."default",
			idprocpte character varying(11) COLLATE pg_catalog."default",
			idparref character varying(14) COLLATE pg_catalog."default",
			idsecref character varying(10) COLLATE pg_catalog."default",
			idvoie character varying(9) COLLATE pg_catalog."default",
			idcom character varying(5) COLLATE pg_catalog."default",
			idcomtxt character varying(45) COLLATE pg_catalog."default",
			ccodep character varying(2) COLLATE pg_catalog."default",
			ccodir character varying(1) COLLATE pg_catalog."default",
			ccocom character varying(3) COLLATE pg_catalog."default",
			ccopre character varying(3) COLLATE pg_catalog."default",
			ccosec character varying(2) COLLATE pg_catalog."default",
			dnupla character varying(4) COLLATE pg_catalog."default",
			dcntpa integer,
			dsrpar character varying(1) COLLATE pg_catalog."default",
			dnupro character varying(6) COLLATE pg_catalog."default",
			jdatat character varying(8) COLLATE pg_catalog."default",
			jdatatv character varying(8) COLLATE pg_catalog."default",
			dreflf character varying(5) COLLATE pg_catalog."default",
			gpdl character varying(1) COLLATE pg_catalog."default",
			cprsecr character varying(3) COLLATE pg_catalog."default",
			ccosecr character varying(2) COLLATE pg_catalog."default",
			dnuplar character varying(4) COLLATE pg_catalog."default",
			dnupdl character varying(3) COLLATE pg_catalog."default",
			gurbpa character varying(1) COLLATE pg_catalog."default",
			dparpi character varying(4) COLLATE pg_catalog."default",
			ccoarp character varying(1) COLLATE pg_catalog."default",
			gparnf character varying(1) COLLATE pg_catalog."default",
			gparbat character varying(1) COLLATE pg_catalog."default",
			dnvoiri character varying(4) COLLATE pg_catalog."default",
			dindic character varying(1) COLLATE pg_catalog."default",
			ccovoi character varying(5) COLLATE pg_catalog."default",
			ccoriv character varying(4) COLLATE pg_catalog."default",
			ccocif character varying(4) COLLATE pg_catalog."default",
			cconvo character varying(4) COLLATE pg_catalog."default",
			dvoilib character varying(26) COLLATE pg_catalog."default",
			idparm character varying(14) COLLATE pg_catalog."default",
			ccocomm integer,
			ccoprem character varying(3) COLLATE pg_catalog."default",
			ccosecm character varying(2) COLLATE pg_catalog."default",
			dnuplam character varying(4) COLLATE pg_catalog."default",
			type character varying(1) COLLATE pg_catalog."default",
			typetxt character varying(16) COLLATE pg_catalog."default",
			jdatatan integer,
			jannatmin integer,
			jannatmax integer,
			jannatminh integer,
			jannatmaxh integer,
			janbilmin integer,
			nsuf integer,
			ssuf integer,
			cgrnumd character varying(2) COLLATE pg_catalog."default",
			cgrnumdtxt character varying(19) COLLATE pg_catalog."default",
			dcntsfd integer,
			dcntsol integer,
			dcntagri integer,
			dcntbois integer,
			dcntnat integer,
			dcnteau integer,
			dcnt01 integer,
			dcnt02 integer,
			dcnt03 integer,
			dcnt04 integer,
			dcnt06 integer,
			dcnt07 integer,
			dcnt09 integer,
			dcnt10 integer,
			dcnt11 integer,
			dcnt12 integer,
			dcnt13 integer,
			schemrem integer,
			nlocal integer,
			nlocmaison integer,
			nlocappt integer,
			nlochabit integer,
			nloccom integer,
			nloccomrdc integer,
			nloccomter integer,
			nloccomsec integer,
			nlocdep integer,
			nlocburx integer,
			tlocdomin character varying(11) COLLATE pg_catalog."default",
			nbat integer,
			npevaffh integer,
			npevph integer,
			stoth integer,
			smoyh integer,
			npiecemoy double precision,
			stotdsueic integer,
			nhabvacant integer,
			nactvacant integer,
			nmediocre integer,
			nloghlm integer,
			noccprop integer,
			nocclocat integer,
			typoocc character varying(13) COLLATE pg_catalog."default",
			npevp integer,
			stotp integer,
			smoyp integer,
			npevd integer,
			stotd integer,
			smoyd integer,
			spevtot integer,
			tpevdom_n character varying(13) COLLATE pg_catalog."default",
			tpevdom_s character varying(14) COLLATE pg_catalog."default",
			nlot integer,
			pdlmp character varying(1) COLLATE pg_catalog."default",
			ctpdl character varying(5) COLLATE pg_catalog."default",
			ndroit integer,
			ndroitindi integer,
			ndroitpro integer,
			ndroitges integer,
			typprop character varying(7) COLLATE pg_catalog."default",
			typproptxt character varying(57) COLLATE pg_catalog."default",
			typproppro character varying(7) COLLATE pg_catalog."default",
			typpropprotxt character varying(57) COLLATE pg_catalog."default",
			typpropges character varying(7) COLLATE pg_catalog."default",
			typpropgestxt character varying(57) COLLATE pg_catalog."default",
			locprop character varying(1) COLLATE pg_catalog."default",
			locproptxt character varying(21) COLLATE pg_catalog."default",
			geomloc geometry(MultiPoint,2154),
			geompar geometry(MultiPolygon,2154),
			source_geo character varying(34) COLLATE pg_catalog."default",
			vecteur character varying(1) COLLATE pg_catalog."default",
			idpk integer NOT NULL,
			--SUITE A CONSERVER
			a_prop_cod_prop bpchar[] COLLATE pg_catalog."default",
			a_prop_id bigint[],
			a_prop_idu text[] COLLATE pg_catalog."default",
			a_prop_public boolean[],
			a_prop_cominterco boolean[],
			a_prop_com_niv character varying[] COLLATE pg_catalog."default",
			a_prop_com_niv_libelle text[] COLLATE pg_catalog."default",
			a_ges_cod_prop bpchar[] COLLATE pg_catalog."default",
			a_ges_id bigint[],
			a_ges_idu text[] COLLATE pg_catalog."default",
			a_ges_public boolean[],
			a_ges_cominterco boolean[],
			a_ges_com_niv character varying[] COLLATE pg_catalog."default",
			a_ges_com_niv_libelle text[] COLLATE pg_catalog."default",
			cominterco boolean,
			com_niv int,
			idcom22 character varying(5) COLLATE pg_catalog."default",
			PRIMARY KEY(ccodep,idpk),
			UNIQUE(ccodep,idpar)
		)
		PARTITION BY LIST(ccodep);
		
		--CREATION D'UNE TABLE TEMPORAIRE DES DEPARTEMENTS
		CREATE SCHEMA IF NOT EXISTS temp;
		DROP TABLE IF EXISTS temp.dep;
		CREATE TABLE temp.dep AS (
			WITH sub AS (
				SELECT par.ccodep
				FROM %I.%I par
				GROUP BY par.ccodep)
			SELECT ARRAY_AGG(ccodep) ccodep
			FROM sub
		);		
		$$, table_name_new, table_name_new, schema_name, table_name_parcelle);

		--Création des partitions par département
		a_ccodep = (SELECT dep.ccodep FROM temp.dep dep);

		FOREACH ccodep IN ARRAY ARRAY[a_ccodep]
		LOOP
			partition_tablename = table_name_new||'_'||ccodep;
		EXECUTE format(
		$$ 
				CREATE TABLE IF NOT EXISTS nat.%I
				PARTITION OF nat.%I
				FOR VALUES IN (%L)
			$$, partition_tablename,table_name_new,ccodep);
		END LOOP;
		
		EXECUTE format(
		$$
			DROP TABLE temp.dep
		$$);
		
END
$do$


---2014 (36s)---

DO
$do$
DECLARE
	millesime text;
	millesime_short text;
	ccodep text;
  	partition_tablename text;
  	a_ccodep text[];
	table_temp_dep text;
	schema_name text;
	table_name_parcelle text;
	table_name_new text;
BEGIN
	millesime = '2014';
	millesime_short = RIGHT(millesime,2);
	table_temp_dep = 'dep'||millesime;
	schema_name = 'ff_'||millesime;
	table_name_parcelle = 'fftp_'||millesime||'_pnb10_parcelle';
	table_name_new = 'public'||millesime_short;
	
	EXECUTE format(
		$$
		--Création de la table des parcelles
		DROP TABLE IF EXISTS nat.%I CASCADE;
		CREATE TABLE IF NOT EXISTS nat.%I(
			idpar character varying(14) COLLATE pg_catalog."default",
			idsec character varying(10) COLLATE pg_catalog."default",
			idprocpte character varying(11) COLLATE pg_catalog."default",
			idparref character varying(14) COLLATE pg_catalog."default",
			idsecref character varying(10) COLLATE pg_catalog."default",
			idvoie character varying(9) COLLATE pg_catalog."default",
			idcom character varying(5) COLLATE pg_catalog."default",
			idcomtxt character varying(45) COLLATE pg_catalog."default",
			ccodep character varying(2) COLLATE pg_catalog."default",
			ccodir character varying(1) COLLATE pg_catalog."default",
			ccocom character varying(3) COLLATE pg_catalog."default",
			ccopre character varying(3) COLLATE pg_catalog."default",
			ccosec character varying(2) COLLATE pg_catalog."default",
			dnupla character varying(4) COLLATE pg_catalog."default",
			dcntpa integer,
			dsrpar character varying(1) COLLATE pg_catalog."default",
			dnupro character varying(6) COLLATE pg_catalog."default",
			jdatat character varying(8) COLLATE pg_catalog."default",
			jdatatv character varying(8) COLLATE pg_catalog."default",
			dreflf character varying(5) COLLATE pg_catalog."default",
			gpdl character varying(1) COLLATE pg_catalog."default",
			cprsecr character varying(3) COLLATE pg_catalog."default",
			ccosecr character varying(2) COLLATE pg_catalog."default",
			dnuplar character varying(4) COLLATE pg_catalog."default",
			dnupdl character varying(3) COLLATE pg_catalog."default",
			gurbpa character varying(1) COLLATE pg_catalog."default",
			dparpi character varying(4) COLLATE pg_catalog."default",
			ccoarp character varying(1) COLLATE pg_catalog."default",
			gparnf character varying(1) COLLATE pg_catalog."default",
			gparbat character varying(1) COLLATE pg_catalog."default",
			dnuvoi character varying(4) COLLATE pg_catalog."default",
			dindic character varying(1) COLLATE pg_catalog."default",
			ccovoi character varying(5) COLLATE pg_catalog."default",
			ccoriv character varying(4) COLLATE pg_catalog."default",
			ccocif character varying(4) COLLATE pg_catalog."default",
			cconvo character varying(4) COLLATE pg_catalog."default",
			dvoilib character varying(26) COLLATE pg_catalog."default",
			idparm character varying(14) COLLATE pg_catalog."default",
			ccocomm integer,
			ccoprem character varying(3) COLLATE pg_catalog."default",
			ccosecm character varying(2) COLLATE pg_catalog."default",
			dnuplam character varying(4) COLLATE pg_catalog."default",
			type character varying(1) COLLATE pg_catalog."default",
			typetxt character varying(16) COLLATE pg_catalog."default",
			jdatatan integer,
			nmutpar5a integer,
			jannatmin integer,
			jannatmax integer,
			jannatminh integer,
			jannatmaxh integer,
			janbilmin integer,
			nsuf integer,
			ssuf integer,
			cgrnumd character varying(2) COLLATE pg_catalog."default",
			cgrnumdtxt character varying(19) COLLATE pg_catalog."default",
			dcntsfd integer,
			dcntsol integer,
			dcntagri integer,
			dcntbois integer,
			dcntnat integer,
			dcnteau integer,
			dcnt01 integer,
			dcnt02 integer,
			dcnt03 integer,
			dcnt04 integer,
			dcnt06 integer,
			dcnt07 integer,
			dcnt09 integer,
			dcnt10 integer,
			dcnt11 integer,
			dcnt12 integer,
			dcnt13 integer,
			schemrem integer,
			nlocal integer,
			nlocmaison integer,
			nlocappt integer,
			nloclog integer,
			nloccom integer,
			nloccomrdc integer,
			nloccomter integer,
			ncomtersd integer,
			ncomterdep integer,
			nloccomsec integer,
			nlocdep integer,
			nlocburx integer,
			tlocdomin character varying(11) COLLATE pg_catalog."default",
			nbat integer,
			nlochab integer,
			nlogh integer,
			npevph integer,
			stoth integer,
			stotdsueic integer,
			nloghvac integer,
			nloghmeu integer,
			nloghloue integer,
			nloghpp integer,
			nloghautre integer,
			nloghnonh integer,
			nhabvacant integer,
			nactvacant integer,
			nloghvac2a integer,
			nactvac2a integer,
			nloghvac5a integer,
			nactvac5a integer,
			nmediocre integer,
			nloghlm integer,
			npevp integer,
			stotp integer,
			npevd integer,
			stotd integer,
			spevtot integer,
			tpevdom_s character varying(14) COLLATE pg_catalog."default",
			nlot integer,
			pdlmp character varying(1) COLLATE pg_catalog."default",
			ctpdl character varying(5) COLLATE pg_catalog."default",
			typecopro character varying(1) COLLATE pg_catalog."default",
			ncp integer,
			ndroit integer,
			ndroitindi integer,
			ndroitpro integer,
			ndroitges integer,
			typprop character varying(7) COLLATE pg_catalog."default",
			typproptxt character varying(57) COLLATE pg_catalog."default",
			typproppro character varying(7) COLLATE pg_catalog."default",
			typpropprotxt character varying(57) COLLATE pg_catalog."default",
			typpropges character varying(7) COLLATE pg_catalog."default",
			typpropgestxt character varying(57) COLLATE pg_catalog."default",
			locprop character varying(1) COLLATE pg_catalog."default",
			locproptxt character varying(21) COLLATE pg_catalog."default",
			geomloc geometry(MultiPoint,2154),
			geompar geometry(MultiPolygon,2154),
			source_geo character varying(34) COLLATE pg_catalog."default",
			vecteur character varying(1) COLLATE pg_catalog."default",
			contour character varying(3) COLLATE pg_catalog."default",
			idpk integer NOT NULL,
			--SUITE A CONSERVER
			a_prop_cod_prop bpchar[] COLLATE pg_catalog."default",
			a_prop_id bigint[],
			a_prop_idu text[] COLLATE pg_catalog."default",
			a_prop_public boolean[],
			a_prop_cominterco boolean[],
			a_prop_com_niv character varying[] COLLATE pg_catalog."default",
			a_prop_com_niv_libelle text[] COLLATE pg_catalog."default",
			a_ges_cod_prop bpchar[] COLLATE pg_catalog."default",
			a_ges_id bigint[],
			a_ges_idu text[] COLLATE pg_catalog."default",
			a_ges_public boolean[],
			a_ges_cominterco boolean[],
			a_ges_com_niv character varying[] COLLATE pg_catalog."default",
			a_ges_com_niv_libelle text[] COLLATE pg_catalog."default",
			cominterco boolean,
			com_niv int,
			idcom22 character varying(5) COLLATE pg_catalog."default",
			PRIMARY KEY(ccodep,idpk),
			UNIQUE(ccodep,idpar)
		)
		PARTITION BY LIST(ccodep);
		
		--CREATION D'UNE TABLE TEMPORAIRE DES DEPARTEMENTS
		CREATE SCHEMA IF NOT EXISTS temp;
		DROP TABLE IF EXISTS temp.dep;
		CREATE TABLE temp.dep AS (
			WITH sub AS (
				SELECT par.ccodep
				FROM %I.%I par
				GROUP BY par.ccodep)
			SELECT ARRAY_AGG(ccodep) ccodep
			FROM sub
		);		
		$$, table_name_new, table_name_new, schema_name, table_name_parcelle);

		--Création des partitions par département
		a_ccodep = (SELECT dep.ccodep FROM temp.dep dep);

		FOREACH ccodep IN ARRAY ARRAY[a_ccodep]
		LOOP
			partition_tablename = table_name_new||'_'||ccodep;
		EXECUTE format(
		$$ 
				CREATE TABLE IF NOT EXISTS nat.%I
				PARTITION OF nat.%I
				FOR VALUES IN (%L)
			$$, partition_tablename,table_name_new,ccodep);
		END LOOP;
		
		EXECUTE format(
		$$
			DROP TABLE temp.dep
		$$);
		
END
$do$


---2015 (37s)---

DO
$do$
DECLARE
	millesime text;
	millesime_short text;
	ccodep text;
  	partition_tablename text;
  	a_ccodep text[];
	table_temp_dep text;
	schema_name text;
	table_name_parcelle text;
	table_name_new text;
BEGIN
	millesime = '2015';
	millesime_short = RIGHT(millesime,2);
	table_temp_dep = 'dep'||millesime;
	schema_name = 'ff_'||millesime;
	table_name_parcelle = 'fftp_'||millesime||'_pnb10_parcelle';
	table_name_new = 'public'||millesime_short;
	
	EXECUTE format(
		$$
		--Création de la table des parcelles
		DROP TABLE IF EXISTS nat.%I CASCADE;
		CREATE TABLE IF NOT EXISTS nat.%I(
			idpar character varying(14) COLLATE pg_catalog."default",
			idsec character varying(10) COLLATE pg_catalog."default",
			idprocpte character varying(11) COLLATE pg_catalog."default",
			idparref character varying(14) COLLATE pg_catalog."default",
			idsecref character varying(10) COLLATE pg_catalog."default",
			idvoie character varying(9) COLLATE pg_catalog."default",
			idcom character varying(5) COLLATE pg_catalog."default",
			idcomtxt character varying(45) COLLATE pg_catalog."default",
			ccodep character varying(2) COLLATE pg_catalog."default",
			ccodir character varying(1) COLLATE pg_catalog."default",
			ccocom character varying(3) COLLATE pg_catalog."default",
			ccopre character varying(3) COLLATE pg_catalog."default",
			ccosec character varying(2) COLLATE pg_catalog."default",
			dnupla character varying(4) COLLATE pg_catalog."default",
			dcntpa integer,
			dsrpar character varying(1) COLLATE pg_catalog."default",
			dnupro character varying(6) COLLATE pg_catalog."default",
			jdatat character varying(8) COLLATE pg_catalog."default",
			jdatatv character varying(8) COLLATE pg_catalog."default",
			dreflf character varying(5) COLLATE pg_catalog."default",
			gpdl character varying(1) COLLATE pg_catalog."default",
			cprsecr character varying(3) COLLATE pg_catalog."default",
			ccosecr character varying(2) COLLATE pg_catalog."default",
			dnuplar character varying(4) COLLATE pg_catalog."default",
			dnupdl character varying(3) COLLATE pg_catalog."default",
			gurbpa character varying(1) COLLATE pg_catalog."default",
			dparpi character varying(4) COLLATE pg_catalog."default",
			ccoarp character varying(1) COLLATE pg_catalog."default",
			gparnf character varying(1) COLLATE pg_catalog."default",
			gparbat character varying(1) COLLATE pg_catalog."default",
			dnuvoi character varying(4) COLLATE pg_catalog."default",
			dindic character varying(1) COLLATE pg_catalog."default",
			ccovoi character varying(5) COLLATE pg_catalog."default",
			ccoriv character varying(4) COLLATE pg_catalog."default",
			ccocif character varying(4) COLLATE pg_catalog."default",
			cconvo character varying(4) COLLATE pg_catalog."default",
			dvoilib character varying(26) COLLATE pg_catalog."default",
			idparm character varying(14) COLLATE pg_catalog."default",
			ccocomm integer,
			ccoprem character varying(3) COLLATE pg_catalog."default",
			ccosecm character varying(2) COLLATE pg_catalog."default",
			dnuplam character varying(4) COLLATE pg_catalog."default",
			type character varying(1) COLLATE pg_catalog."default",
			typetxt character varying(16) COLLATE pg_catalog."default",
			jdatatan integer,
			jannatmin integer,
			jannatmax integer,
			jannatminh integer,
			jannatmaxh integer,
			janbilmin integer,
			nsuf integer,
			ssuf integer,
			cgrnumd character varying(2) COLLATE pg_catalog."default",
			cgrnumdtxt character varying(19) COLLATE pg_catalog."default",
			dcntsfd integer,
			dcntsol integer,
			dcntagri integer,
			dcntbois integer,
			dcntnat integer,
			dcnteau integer,
			dcnt01 integer,
			dcnt02 integer,
			dcnt03 integer,
			dcnt04 integer,
			dcnt06 integer,
			dcnt07 integer,
			dcnt09 integer,
			dcnt10 integer,
			dcnt11 integer,
			dcnt12 integer,
			dcnt13 integer,
			schemrem integer,
			nlocal integer,
			nlocmaison integer,
			nlocappt integer,
			nloclog integer,
			nloccom integer,
			nloccomrdc integer,
			nloccomter integer,
			ncomtersd integer,
			ncomterdep integer,
			nloccomsec integer,
			nlocdep integer,
			nlocburx integer,
			tlocdomin character varying(11) COLLATE pg_catalog."default",
			nbat integer,
			nlochab integer,
			nlogh integer,
			npevph integer,
			stoth integer,
			stotdsueic integer,
			nloghvac integer,
			nloghmeu integer,
			nloghloue integer,
			nloghpp integer,
			nloghautre integer,
			nloghnonh integer,
			nhabvacant integer,
			nactvacant integer,
			nloghvac2a integer,
			nactvac2a integer,
			nloghvac5a integer,
			nactvac5a integer,
			nmediocre integer,
			nloghlm integer,
			npevp integer,
			stotp integer,
			npevd integer,
			stotd integer,
			spevtot integer,
			tpevdom_s character varying(14) COLLATE pg_catalog."default",
			nlot integer,
			pdlmp character varying(1) COLLATE pg_catalog."default",
			ctpdl character varying(5) COLLATE pg_catalog."default",
			typecopro character varying(1) COLLATE pg_catalog."default",
			ncp integer,
			ndroit integer,
			ndroitindi integer,
			ndroitpro integer,
			ndroitges integer,
			typprop character varying(7) COLLATE pg_catalog."default",
			typproptxt character varying(57) COLLATE pg_catalog."default",
			typproppro character varying(7) COLLATE pg_catalog."default",
			typpropprotxt character varying(57) COLLATE pg_catalog."default",
			typpropges character varying(7) COLLATE pg_catalog."default",
			typpropgestxt character varying(57) COLLATE pg_catalog."default",
			locprop character varying(1) COLLATE pg_catalog."default",
			locproptxt character varying(21) COLLATE pg_catalog."default",
			geomloc geometry(MultiPoint,2154),
			geompar geometry(MultiPolygon,2154),
			source_geo character varying(34) COLLATE pg_catalog."default",
			vecteur character varying(1) COLLATE pg_catalog."default",
			contour character varying(3) COLLATE pg_catalog."default",
			idpk integer NOT NULL,
			--SUITE A CONSERVER
			a_prop_cod_prop bpchar[] COLLATE pg_catalog."default",
			a_prop_id bigint[],
			a_prop_idu text[] COLLATE pg_catalog."default",
			a_prop_public boolean[],
			a_prop_cominterco boolean[],
			a_prop_com_niv character varying[] COLLATE pg_catalog."default",
			a_prop_com_niv_libelle text[] COLLATE pg_catalog."default",
			a_ges_cod_prop bpchar[] COLLATE pg_catalog."default",
			a_ges_id bigint[],
			a_ges_idu text[] COLLATE pg_catalog."default",
			a_ges_public boolean[],
			a_ges_cominterco boolean[],
			a_ges_com_niv character varying[] COLLATE pg_catalog."default",
			a_ges_com_niv_libelle text[] COLLATE pg_catalog."default",
			cominterco boolean,
			com_niv int,
			idcom22 character varying(5) COLLATE pg_catalog."default",
			PRIMARY KEY(ccodep,idpk),
			UNIQUE(ccodep,idpar)
		)
		PARTITION BY LIST(ccodep);
		
		--CREATION D'UNE TABLE TEMPORAIRE DES DEPARTEMENTS
		CREATE SCHEMA IF NOT EXISTS temp;
		DROP TABLE IF EXISTS temp.dep;
		CREATE TABLE temp.dep AS (
			WITH sub AS (
				SELECT par.ccodep
				FROM %I.%I par
				GROUP BY par.ccodep)
			SELECT ARRAY_AGG(ccodep) ccodep
			FROM sub
		);		
		$$, table_name_new, table_name_new, schema_name, table_name_parcelle);

		--Création des partitions par département
		a_ccodep = (SELECT dep.ccodep FROM temp.dep dep);

		FOREACH ccodep IN ARRAY ARRAY[a_ccodep]
		LOOP
			partition_tablename = table_name_new||'_'||ccodep;
		EXECUTE format(
		$$ 
				CREATE TABLE IF NOT EXISTS nat.%I
				PARTITION OF nat.%I
				FOR VALUES IN (%L)
			$$, partition_tablename,table_name_new,ccodep);
		END LOOP;
		
		EXECUTE format(
		$$
			DROP TABLE temp.dep
		$$);
		
END
$do$


---2016 (38s)---

DO
$do$
DECLARE
	millesime text;
	millesime_short text;
	ccodep text;
  	partition_tablename text;
  	a_ccodep text[];
	table_temp_dep text;
	schema_name text;
	table_name_parcelle text;
	table_name_new text;
BEGIN
	millesime = '2016';
	millesime_short = RIGHT(millesime,2);
	table_temp_dep = 'dep'||millesime;
	schema_name = 'ff_'||millesime;
	table_name_parcelle = 'fftp_'||millesime||'_pnb10_parcelle';
	table_name_new = 'public'||millesime_short;
	
	EXECUTE format(
		$$
		--Création de la table des parcelles
		DROP TABLE IF EXISTS nat.%I CASCADE;
		CREATE TABLE IF NOT EXISTS nat.%I(
			idpar character varying(14) COLLATE pg_catalog."default",
			idsec character varying(10) COLLATE pg_catalog."default",
			idprocpte character varying(11) COLLATE pg_catalog."default",
			idparref character varying(14) COLLATE pg_catalog."default",
			idsecref character varying(10) COLLATE pg_catalog."default",
			idvoie character varying(9) COLLATE pg_catalog."default",
			idcom character varying(5) COLLATE pg_catalog."default",
			idcomtxt character varying(45) COLLATE pg_catalog."default",
			ccodep character varying(2) COLLATE pg_catalog."default",
			ccodir character varying(1) COLLATE pg_catalog."default",
			ccocom character varying(3) COLLATE pg_catalog."default",
			ccopre character varying(3) COLLATE pg_catalog."default",
			ccosec character varying(2) COLLATE pg_catalog."default",
			dnupla character varying(4) COLLATE pg_catalog."default",
			dcntpa integer,
			dsrpar character varying(1) COLLATE pg_catalog."default",
			dnupro character varying(6) COLLATE pg_catalog."default",
			jdatat character varying(8) COLLATE pg_catalog."default",
			jdatatv character varying(8) COLLATE pg_catalog."default",
			dreflf character varying(5) COLLATE pg_catalog."default",
			gpdl character varying(1) COLLATE pg_catalog."default",
			cprsecr character varying(3) COLLATE pg_catalog."default",
			ccosecr character varying(2) COLLATE pg_catalog."default",
			dnuplar character varying(4) COLLATE pg_catalog."default",
			dnupdl character varying(3) COLLATE pg_catalog."default",
			gurbpa character varying(1) COLLATE pg_catalog."default",
			dparpi character varying(4) COLLATE pg_catalog."default",
			ccoarp character varying(1) COLLATE pg_catalog."default",
			gparnf character varying(1) COLLATE pg_catalog."default",
			gparbat character varying(1) COLLATE pg_catalog."default",
			dnuvoi character varying(4) COLLATE pg_catalog."default",
			dindic character varying(1) COLLATE pg_catalog."default",
			ccovoi character varying(5) COLLATE pg_catalog."default",
			ccoriv character varying(4) COLLATE pg_catalog."default",
			ccocif character varying(4) COLLATE pg_catalog."default",
			cconvo character varying(4) COLLATE pg_catalog."default",
			dvoilib character varying(26) COLLATE pg_catalog."default",
			idparm character varying(14) COLLATE pg_catalog."default",
			ccocomm integer,
			ccoprem character varying(3) COLLATE pg_catalog."default",
			ccosecm character varying(2) COLLATE pg_catalog."default",
			dnuplam character varying(4) COLLATE pg_catalog."default",
			type character varying(1) COLLATE pg_catalog."default",
			typetxt character varying(16) COLLATE pg_catalog."default",
			jdatatan integer,
			jannatmin integer,
			jannatmax integer,
			jannatminh integer,
			jannatmaxh integer,
			janbilmin integer,
			nsuf integer,
			ssuf integer,
			cgrnumd character varying(2) COLLATE pg_catalog."default",
			cgrnumdtxt character varying(19) COLLATE pg_catalog."default",
			dcntsfd integer,
			dcntsol integer,
			dcntarti integer,
			dcntagri integer,
			dcntbois integer,
			dcntnat integer,
			dcnteau integer,
			dcnt01 integer,
			dcnt02 integer,
			dcnt03 integer,
			dcnt04 integer,
			dcnt06 integer,
			dcnt07 integer,
			dcnt09 integer,
			dcnt10 integer,
			dcnt11 integer,
			dcnt12 integer,
			dcnt13 integer,
			schemrem integer,
			nlocal integer,
			nlocmaison integer,
			nlocappt integer,
			nloclog integer,
			nloccom integer,
			nloccomrdc integer,
			nloccomter integer,
			ncomtersd integer,
			ncomterdep integer,
			nloccomsec integer,
			nlocdep integer,
			nlocburx integer,
			tlocdomin character varying(11) COLLATE pg_catalog."default",
			nbat integer,
			nlochab integer,
			nlogh integer,
			npevph integer,
			stoth integer,
			stotdsueic integer,
			nloghvac integer,
			nloghmeu integer,
			nloghloue integer,
			nloghpp integer,
			nloghautre integer,
			nloghnonh integer,
			nhabvacant integer,
			nactvacant integer,
			nloghvac2a integer,
			nactvac2a integer,
			nloghvac5a integer,
			nactvac5a integer,
			nmediocre integer,
			nloghlm integer,
			nloghlls integer,
			npevp integer,
			stotp integer,
			npevd integer,
			stotd integer,
			spevtot integer,
			tpevdom_s character varying(14) COLLATE pg_catalog."default",
			nlot integer,
			pdlmp character varying(1) COLLATE pg_catalog."default",
			ctpdl character varying(5) COLLATE pg_catalog."default",
			typecopro character varying(1) COLLATE pg_catalog."default",
			ncp integer,
			ndroit integer,
			ndroitindi integer,
			ndroitpro integer,
			ndroitges integer,
			typprop character varying(7) COLLATE pg_catalog."default",
			typproptxt character varying(57) COLLATE pg_catalog."default",
			typproppro character varying(7) COLLATE pg_catalog."default",
			typpropprotxt character varying(57) COLLATE pg_catalog."default",
			typpropges character varying(7) COLLATE pg_catalog."default",
			typpropgestxt character varying(57) COLLATE pg_catalog."default",
			locprop character varying(1) COLLATE pg_catalog."default",
			locproptxt character varying(21) COLLATE pg_catalog."default",
			geomloc geometry(MultiPoint,2154),
			geompar geometry(MultiPolygon,2154),
			source_geo character varying(34) COLLATE pg_catalog."default",
			vecteur character varying(1) COLLATE pg_catalog."default",
			contour character varying(3) COLLATE pg_catalog."default",
			idpk integer NOT NULL,
			--SUITE A CONSERVER
			a_prop_cod_prop bpchar[] COLLATE pg_catalog."default",
			a_prop_id bigint[],
			a_prop_idu text[] COLLATE pg_catalog."default",
			a_prop_public boolean[],
			a_prop_cominterco boolean[],
			a_prop_com_niv character varying[] COLLATE pg_catalog."default",
			a_prop_com_niv_libelle text[] COLLATE pg_catalog."default",
			a_ges_cod_prop bpchar[] COLLATE pg_catalog."default",
			a_ges_id bigint[],
			a_ges_idu text[] COLLATE pg_catalog."default",
			a_ges_public boolean[],
			a_ges_cominterco boolean[],
			a_ges_com_niv character varying[] COLLATE pg_catalog."default",
			a_ges_com_niv_libelle text[] COLLATE pg_catalog."default",
			cominterco boolean,
			com_niv int,
			idcom22 character varying(5) COLLATE pg_catalog."default",
			PRIMARY KEY(ccodep,idpk),
			UNIQUE(ccodep,idpar)
		)
		PARTITION BY LIST(ccodep);
		
		--CREATION D'UNE TABLE TEMPORAIRE DES DEPARTEMENTS
		CREATE SCHEMA IF NOT EXISTS temp;
		DROP TABLE IF EXISTS temp.dep;
		CREATE TABLE temp.dep AS (
			WITH sub AS (
				SELECT par.ccodep
				FROM %I.%I par
				GROUP BY par.ccodep)
			SELECT ARRAY_AGG(ccodep) ccodep
			FROM sub
		);		
		$$, table_name_new, table_name_new, schema_name, table_name_parcelle);

		--Création des partitions par département
		a_ccodep = (SELECT dep.ccodep FROM temp.dep dep);

		FOREACH ccodep IN ARRAY ARRAY[a_ccodep]
		LOOP
			partition_tablename = table_name_new||'_'||ccodep;
		EXECUTE format(
		$$ 
				CREATE TABLE IF NOT EXISTS nat.%I
				PARTITION OF nat.%I
				FOR VALUES IN (%L)
			$$, partition_tablename,table_name_new,ccodep);
		END LOOP;
		
		EXECUTE format(
		$$
			DROP TABLE temp.dep
		$$);
		
END
$do$


---2017 (39s)---

DO
$do$
DECLARE
	millesime text;
	millesime_short text;
	ccodep text;
  	partition_tablename text;
  	a_ccodep text[];
	table_temp_dep text;
	schema_name text;
	table_name_parcelle text;
	table_name_new text;
BEGIN
	millesime = '2017';
	millesime_short = RIGHT(millesime,2);
	table_temp_dep = 'dep'||millesime;
	schema_name = 'ff_'||millesime;
	table_name_parcelle = 'fftp_'||millesime||'_pnb10_parcelle';
	table_name_new = 'public'||millesime_short;
	
	EXECUTE format(
		$$
		--Création de la table des parcelles
		DROP TABLE IF EXISTS nat.%I CASCADE;
		CREATE TABLE IF NOT EXISTS nat.%I(
			idpar character varying(14) COLLATE pg_catalog."default",
			idsec character varying(10) COLLATE pg_catalog."default",
			idprocpte character varying(11) COLLATE pg_catalog."default",
			idparref character varying(14) COLLATE pg_catalog."default",
			idsecref character varying(10) COLLATE pg_catalog."default",
			idvoie character varying(9) COLLATE pg_catalog."default",
			idcom character varying(5) COLLATE pg_catalog."default",
			idcomtxt character varying(45) COLLATE pg_catalog."default",
			ccodep character varying(2) COLLATE pg_catalog."default",
			ccodir character varying(1) COLLATE pg_catalog."default",
			ccocom character varying(3) COLLATE pg_catalog."default",
			ccopre character varying(3) COLLATE pg_catalog."default",
			ccosec character varying(2) COLLATE pg_catalog."default",
			dnupla character varying(4) COLLATE pg_catalog."default",
			dcntpa integer,
			dsrpar character varying(1) COLLATE pg_catalog."default",
			dnupro character varying(6) COLLATE pg_catalog."default",
			jdatat character varying(8) COLLATE pg_catalog."default",
			jdatatv character varying(8) COLLATE pg_catalog."default",
			dreflf character varying(5) COLLATE pg_catalog."default",
			gpdl character varying(1) COLLATE pg_catalog."default",
			cprsecr character varying(3) COLLATE pg_catalog."default",
			ccosecr character varying(2) COLLATE pg_catalog."default",
			dnuplar character varying(4) COLLATE pg_catalog."default",
			dnupdl character varying(3) COLLATE pg_catalog."default",
			gurbpa character varying(1) COLLATE pg_catalog."default",
			dparpi character varying(4) COLLATE pg_catalog."default",
			ccoarp character varying(1) COLLATE pg_catalog."default",
			gparnf character varying(1) COLLATE pg_catalog."default",
			gparbat character varying(1) COLLATE pg_catalog."default",
			dnuvoi character varying(4) COLLATE pg_catalog."default",
			dindic character varying(1) COLLATE pg_catalog."default",
			ccovoi character varying(5) COLLATE pg_catalog."default",
			ccoriv character varying(4) COLLATE pg_catalog."default",
			ccocif character varying(4) COLLATE pg_catalog."default",
			cconvo character varying(4) COLLATE pg_catalog."default",
			dvoilib character varying(26) COLLATE pg_catalog."default",
			idparm character varying(14) COLLATE pg_catalog."default",
			ccocomm integer,
			ccoprem character varying(3) COLLATE pg_catalog."default",
			ccosecm character varying(2) COLLATE pg_catalog."default",
			dnuplam character varying(4) COLLATE pg_catalog."default",
			type character varying(1) COLLATE pg_catalog."default",
			typetxt character varying(16) COLLATE pg_catalog."default",
			ccoifp integer,
			jdatatan integer,
			jannatmin integer,
			jannatmax integer,
			jannatminh integer,
			jannatmaxh integer,
			janbilmin integer,
			nsuf integer,
			ssuf integer,
			cgrnumd character varying(2) COLLATE pg_catalog."default",
			cgrnumdtxt character varying(19) COLLATE pg_catalog."default",
			dcntsfd integer,
			dcntarti integer,
			dcntnaf integer,
			dcnt01 integer,
			dcnt02 integer,
			dcnt03 integer,
			dcnt04 integer,
			dcnt05 integer,
			dcnt06 integer,
			dcnt07 integer,
			dcnt08 integer,
			dcnt09 integer,
			dcnt10 integer,
			dcnt11 integer,
			dcnt12 integer,
			dcnt13 integer,
			schemrem integer,
			nlocal integer,
			nlocmaison integer,
			nlocappt integer,
			nloclog integer,
			nloccom integer,
			nloccomrdc integer,
			nloccomter integer,
			ncomtersd integer,
			ncomterdep integer,
			nloccomsec integer,
			nlocdep integer,
			nlocburx integer,
			tlocdomin character varying(11) COLLATE pg_catalog."default",
			nbat integer,
			nlochab integer,
			nlogh integer,
			npevph integer,
			stoth integer,
			stotdsueic integer,
			nloghvac integer,
			nloghmeu integer,
			nloghloue integer,
			nloghpp integer,
			nloghautre integer,
			nloghnonh integer,
			nhabvacant integer,
			nactvacant integer,
			nloghvac2a integer,
			nactvac2a integer,
			nloghvac5a integer,
			nactvac5a integer,
			nmediocre integer,
			nloghlm integer,
			nloghlls integer,
			npevp integer,
			sprobati integer,
			sprotot integer,
			npevd integer,
			stotd integer,
			spevtot integer,
			tpevdom_s character varying(14) COLLATE pg_catalog."default",
			nlot integer,
			pdlmp character varying(1) COLLATE pg_catalog."default",
			ctpdl character varying(5) COLLATE pg_catalog."default",
			typecopro2 character varying(1) COLLATE pg_catalog."default",
			ncp integer,
			ndroit integer,
			ndroitindi integer,
			ndroitpro integer,
			ndroitges integer,
			typprop character varying(7) COLLATE pg_catalog."default",
			typproptxt character varying(57) COLLATE pg_catalog."default",
			typproppro character varying(7) COLLATE pg_catalog."default",
			typpropprotxt character varying(57) COLLATE pg_catalog."default",
			typpropges character varying(7) COLLATE pg_catalog."default",
			typpropgestxt character varying(57) COLLATE pg_catalog."default",
			locprop character varying(1) COLLATE pg_catalog."default",
			locproptxt character varying(21) COLLATE pg_catalog."default",
			geomloc geometry(MultiPoint,2154),
			geompar geometry(MultiPolygon,2154),
			source_geo character varying(34) COLLATE pg_catalog."default",
			vecteur character varying(1) COLLATE pg_catalog."default",
			contour character varying(3) COLLATE pg_catalog."default",
			idpk integer NOT NULL,
			--SUITE A CONSERVER
			a_prop_cod_prop bpchar[] COLLATE pg_catalog."default",
			a_prop_id bigint[],
			a_prop_idu text[] COLLATE pg_catalog."default",
			a_prop_public boolean[],
			a_prop_cominterco boolean[],
			a_prop_com_niv character varying[] COLLATE pg_catalog."default",
			a_prop_com_niv_libelle text[] COLLATE pg_catalog."default",
			a_ges_cod_prop bpchar[] COLLATE pg_catalog."default",
			a_ges_id bigint[],
			a_ges_idu text[] COLLATE pg_catalog."default",
			a_ges_public boolean[],
			a_ges_cominterco boolean[],
			a_ges_com_niv character varying[] COLLATE pg_catalog."default",
			a_ges_com_niv_libelle text[] COLLATE pg_catalog."default",
			cominterco boolean,
			com_niv int,
			idcom22 character varying(5) COLLATE pg_catalog."default",
			PRIMARY KEY(ccodep,idpk),
			UNIQUE(ccodep,idpar)
		)
		PARTITION BY LIST(ccodep);
		
		--CREATION D'UNE TABLE TEMPORAIRE DES DEPARTEMENTS
		CREATE SCHEMA IF NOT EXISTS temp;
		DROP TABLE IF EXISTS temp.dep;
		CREATE TABLE temp.dep AS (
			WITH sub AS (
				SELECT par.ccodep
				FROM %I.%I par
				GROUP BY par.ccodep)
			SELECT ARRAY_AGG(ccodep) ccodep
			FROM sub
		);		
		$$, table_name_new, table_name_new, schema_name, table_name_parcelle);

		--Création des partitions par département
		a_ccodep = (SELECT dep.ccodep FROM temp.dep dep);

		FOREACH ccodep IN ARRAY ARRAY[a_ccodep]
		LOOP
			partition_tablename = table_name_new||'_'||ccodep;
		EXECUTE format(
		$$ 
				CREATE TABLE IF NOT EXISTS nat.%I
				PARTITION OF nat.%I
				FOR VALUES IN (%L)
			$$, partition_tablename,table_name_new,ccodep);
		END LOOP;
		
		EXECUTE format(
		$$
			DROP TABLE temp.dep
		$$);
		
END
$do$


---2018 (36s)---

DO
$do$
DECLARE
	millesime text;
	millesime_short text;
	ccodep text;
  	partition_tablename text;
  	a_ccodep text[];
	table_temp_dep text;
	schema_name text;
	table_name_parcelle text;
	table_name_new text;
BEGIN
	millesime = '2018';
	millesime_short = RIGHT(millesime,2);
	table_temp_dep = 'dep'||millesime;
	schema_name = 'ff_'||millesime;
	table_name_parcelle = 'fftp_'||millesime||'_pnb10_parcelle';
	table_name_new = 'public'||millesime_short;
	
	EXECUTE format(
		$$
		--Création de la table des parcelles
		DROP TABLE IF EXISTS nat.%I CASCADE;
		CREATE TABLE IF NOT EXISTS nat.%I(
			idpar character varying(14) COLLATE pg_catalog."default",
			idsec character varying(10) COLLATE pg_catalog."default",
			idprocpte character varying(11) COLLATE pg_catalog."default",
			idparref character varying(14) COLLATE pg_catalog."default",
			idsecref character varying(10) COLLATE pg_catalog."default",
			idvoie character varying(9) COLLATE pg_catalog."default",
			idcom character varying(5) COLLATE pg_catalog."default",
			idcomtxt character varying(45) COLLATE pg_catalog."default",
			ccodep character varying(2) COLLATE pg_catalog."default",
			ccodir character varying(1) COLLATE pg_catalog."default",
			ccocom character varying(3) COLLATE pg_catalog."default",
			ccopre character varying(3) COLLATE pg_catalog."default",
			ccosec character varying(2) COLLATE pg_catalog."default",
			dnupla character varying(4) COLLATE pg_catalog."default",
			dcntpa integer,
			dnupro character varying(6) COLLATE pg_catalog."default",
			jdatat character varying(8) COLLATE pg_catalog."default",
			jdatatv character varying(8) COLLATE pg_catalog."default",
			dreflf character varying(5) COLLATE pg_catalog."default",
			gpdl character varying(1) COLLATE pg_catalog."default",
			cprsecr character varying(3) COLLATE pg_catalog."default",
			ccosecr character varying(2) COLLATE pg_catalog."default",
			dnuplar character varying(4) COLLATE pg_catalog."default",
			dnupdl character varying(3) COLLATE pg_catalog."default",
			gurbpa character varying(1) COLLATE pg_catalog."default",
			dparpi character varying(4) COLLATE pg_catalog."default",
			ccoarp character varying(1) COLLATE pg_catalog."default",
			gparnf character varying(1) COLLATE pg_catalog."default",
			gparbat character varying(1) COLLATE pg_catalog."default",
			dnuvoi character varying(4) COLLATE pg_catalog."default",
			dindic character varying(1) COLLATE pg_catalog."default",
			ccovoi character varying(5) COLLATE pg_catalog."default",
			ccoriv character varying(4) COLLATE pg_catalog."default",
			ccocif character varying(4) COLLATE pg_catalog."default",
			cconvo character varying(4) COLLATE pg_catalog."default",
			dvoilib character varying(26) COLLATE pg_catalog."default",
			idparm character varying(14) COLLATE pg_catalog."default",
			ccocomm integer,
			ccoprem character varying(3) COLLATE pg_catalog."default",
			ccosecm character varying(2) COLLATE pg_catalog."default",
			dnuplam character varying(4) COLLATE pg_catalog."default",
			type character varying(1) COLLATE pg_catalog."default",
			typetxt character varying(16) COLLATE pg_catalog."default",
			ccoifp integer,
			jdatatan integer,
			jannatmin integer,
			jannatmax integer,
			jannatminh integer,
			jannatmaxh integer,
			janbilmin integer,
			nsuf integer,
			ssuf integer,
			cgrnumd character varying(2) COLLATE pg_catalog."default",
			cgrnumdtxt character varying(19) COLLATE pg_catalog."default",
			dcntsfd integer,
			dcntarti integer,
			dcntnaf double precision,
			dcnt01 integer,
			dcnt02 integer,
			dcnt03 integer,
			dcnt04 integer,
			dcnt05 integer,
			dcnt06 integer,
			dcnt07 integer,
			dcnt08 integer,
			dcnt09 integer,
			dcnt10 integer,
			dcnt11 integer,
			dcnt12 integer,
			dcnt13 integer,
			schemrem integer,
			nlocal integer,
			nlocmaison integer,
			nlocappt integer,
			nloclog integer,
			nloccom integer,
			nloccomrdc integer,
			nloccomter integer,
			ncomtersd integer,
			ncomterdep integer,
			nloccomsec integer,
			nlocdep integer,
			nlocburx integer,
			tlocdomin character varying(11) COLLATE pg_catalog."default",
			nbat integer,
			nlochab integer,
			nlogh integer,
			npevph integer,
			stoth integer,
			stotdsueic integer,
			nloghvac integer,
			nloghmeu integer,
			nloghloue integer,
			nloghpp integer,
			nloghautre integer,
			nloghnonh integer,
			nactvacant integer,
			nloghvac2a integer,
			nactvac2a integer,
			nloghvac5a integer,
			nactvac5a integer,
			nmediocre integer,
			nloghlm integer,
			nloghlls integer,
			npevd integer,
			stotd integer,
			npevp integer,
			sprincp integer,
			ssecp integer,
			ssecncp integer,
			sparkp integer,
			sparkncp integer,
			slocal integer,
			tpevdom_s character varying(14) COLLATE pg_catalog."default",
			nlot integer,
			pdlmp character varying(1) COLLATE pg_catalog."default",
			ctpdl character varying(5) COLLATE pg_catalog."default",
			typecopro2 character varying(1) COLLATE pg_catalog."default",
			ncp integer,
			ndroit integer,
			ndroitindi integer,
			ndroitpro integer,
			ndroitges integer,
			catpro2 character varying(20) COLLATE pg_catalog."default",
			catpro2txt character varying(200) COLLATE pg_catalog."default",
			catpro3 character varying(30) COLLATE pg_catalog."default",
			catpropro2 character varying(20) COLLATE pg_catalog."default",
			catproges2 character varying(30) COLLATE pg_catalog."default",
			locprop character varying(1) COLLATE pg_catalog."default",
			locproptxt character varying(21) COLLATE pg_catalog."default",
			geompar geometry(MultiPolygon,2154),
			geomloc geometry(Point,2154),
			source_geo character varying(34) COLLATE pg_catalog."default",
			vecteur character varying(1) COLLATE pg_catalog."default",
			contour character varying(3) COLLATE pg_catalog."default",
			idpk integer NOT NULL,
			--SUITE A CONSERVER
			a_prop_cod_prop bpchar[] COLLATE pg_catalog."default",
			a_prop_id bigint[],
			a_prop_idu text[] COLLATE pg_catalog."default",
			a_prop_public boolean[],
			a_prop_cominterco boolean[],
			a_prop_com_niv character varying[] COLLATE pg_catalog."default",
			a_prop_com_niv_libelle text[] COLLATE pg_catalog."default",
			a_ges_cod_prop bpchar[] COLLATE pg_catalog."default",
			a_ges_id bigint[],
			a_ges_idu text[] COLLATE pg_catalog."default",
			a_ges_public boolean[],
			a_ges_cominterco boolean[],
			a_ges_com_niv character varying[] COLLATE pg_catalog."default",
			a_ges_com_niv_libelle text[] COLLATE pg_catalog."default",
			cominterco boolean,
			com_niv int,
			idcom22 character varying(5) COLLATE pg_catalog."default",
			PRIMARY KEY(ccodep,idpk),
			UNIQUE(ccodep,idpar)
		)
		PARTITION BY LIST(ccodep);
		
		--CREATION D'UNE TABLE TEMPORAIRE DES DEPARTEMENTS
		CREATE SCHEMA IF NOT EXISTS temp;
		DROP TABLE IF EXISTS temp.dep;
		CREATE TABLE temp.dep AS (
			WITH sub AS (
				SELECT par.ccodep
				FROM %I.%I par
				GROUP BY par.ccodep)
			SELECT ARRAY_AGG(ccodep) ccodep
			FROM sub
		);		
		$$, table_name_new, table_name_new, schema_name, table_name_parcelle);

		--Création des partitions par département
		a_ccodep = (SELECT dep.ccodep FROM temp.dep dep);

		FOREACH ccodep IN ARRAY ARRAY[a_ccodep]
		LOOP
			partition_tablename = table_name_new||'_'||ccodep;
		EXECUTE format(
		$$ 
				CREATE TABLE IF NOT EXISTS nat.%I
				PARTITION OF nat.%I
				FOR VALUES IN (%L)
			$$, partition_tablename,table_name_new,ccodep);
		END LOOP;
		
		EXECUTE format(
		$$
			DROP TABLE temp.dep
		$$);
		
END
$do$


---2019 (39s)---

DO
$do$
DECLARE
	millesime text;
	millesime_short text;
	ccodep text;
  	partition_tablename text;
  	a_ccodep text[];
	table_temp_dep text;
	schema_name text;
	table_name_parcelle text;
	table_name_new text;
BEGIN
	millesime = '2019';
	millesime_short = RIGHT(millesime,2);
	table_temp_dep = 'dep'||millesime;
	schema_name = 'ff_'||millesime;
	table_name_parcelle = 'fftp_'||millesime||'_pnb10_parcelle';
	table_name_new = 'public'||millesime_short;
	
	EXECUTE format(
		$$
		--Création de la table des parcelles
		DROP TABLE IF EXISTS nat.%I CASCADE;
		CREATE TABLE IF NOT EXISTS nat.%I(
			idpar character varying(14) COLLATE pg_catalog."default",
			idsec character varying(10) COLLATE pg_catalog."default",
			idprocpte character varying(11) COLLATE pg_catalog."default",
			idparref character varying(14) COLLATE pg_catalog."default",
			idsecref character varying(10) COLLATE pg_catalog."default",
			idvoie character varying(9) COLLATE pg_catalog."default",
			idcom character varying(5) COLLATE pg_catalog."default",
			idcomtxt character varying(45) COLLATE pg_catalog."default",
			ccodep character varying(2) COLLATE pg_catalog."default",
			ccodir character varying(1) COLLATE pg_catalog."default",
			ccocom character varying(3) COLLATE pg_catalog."default",
			ccopre character varying(3) COLLATE pg_catalog."default",
			ccosec character varying(2) COLLATE pg_catalog."default",
			dnupla character varying(4) COLLATE pg_catalog."default",
			dcntpa integer,
			dnupro character varying(6) COLLATE pg_catalog."default",
			jdatat character varying(8) COLLATE pg_catalog."default",
			jdatatv character varying(8) COLLATE pg_catalog."default",
			dreflf character varying(5) COLLATE pg_catalog."default",
			gpdl character varying(1) COLLATE pg_catalog."default",
			cprsecr character varying(3) COLLATE pg_catalog."default",
			ccosecr character varying(2) COLLATE pg_catalog."default",
			dnuplar character varying(4) COLLATE pg_catalog."default",
			dnupdl character varying(3) COLLATE pg_catalog."default",
			gurbpa character varying(1) COLLATE pg_catalog."default",
			dparpi character varying(4) COLLATE pg_catalog."default",
			ccoarp character varying(1) COLLATE pg_catalog."default",
			gparnf character varying(1) COLLATE pg_catalog."default",
			gparbat character varying(1) COLLATE pg_catalog."default",
			dnuvoi character varying(4) COLLATE pg_catalog."default",
			dindic character varying(1) COLLATE pg_catalog."default",
			ccovoi character varying(5) COLLATE pg_catalog."default",
			ccoriv character varying(4) COLLATE pg_catalog."default",
			ccocif character varying(4) COLLATE pg_catalog."default",
			ccpper character varying(3) COLLATE pg_catalog."default",
			cconvo character varying(4) COLLATE pg_catalog."default",
			dvoilib character varying(26) COLLATE pg_catalog."default",
			idparm character varying(14) COLLATE pg_catalog."default",
			ccocomm integer,
			ccoprem character varying(3) COLLATE pg_catalog."default",
			ccosecm character varying(2) COLLATE pg_catalog."default",
			dnuplam character varying(4) COLLATE pg_catalog."default",
			type character varying(1) COLLATE pg_catalog."default",
			typetxt character varying(16) COLLATE pg_catalog."default",
			ccoifp integer,
			jdatatan integer,
			jannatmin integer,
			jannatmax integer,
			jannatminh integer,
			jannatmaxh integer,
			janbilmin integer,
			nsuf integer,
			ssuf integer,
			cgrnumd character varying(2) COLLATE pg_catalog."default",
			cgrnumdtxt character varying(19) COLLATE pg_catalog."default",
			dcntsfd integer,
			dcntarti integer,
			dcntnaf double precision,
			dcnt01 integer,
			dcnt02 integer,
			dcnt03 integer,
			dcnt04 integer,
			dcnt05 integer,
			dcnt06 integer,
			dcnt07 integer,
			dcnt08 integer,
			dcnt09 integer,
			dcnt10 integer,
			dcnt11 integer,
			dcnt12 integer,
			dcnt13 integer,
			schemrem integer,
			nlocal integer,
			nlocmaison integer,
			nlocappt integer,
			nloclog integer,
			nloccom integer,
			nloccomrdc integer,
			nloccomter integer,
			ncomtersd integer,
			ncomterdep integer,
			nloccomsec integer,
			nlocdep integer,
			nlocburx integer,
			tlocdomin character varying(11) COLLATE pg_catalog."default",
			nbat integer,
			nlochab integer,
			nlogh integer,
			npevph integer,
			stoth integer,
			stotdsueic integer,
			nloghvac integer,
			nloghmeu integer,
			nloghloue integer,
			nloghpp integer,
			nloghautre integer,
			nloghnonh integer,
			nactvacant integer,
			nloghvac2a integer,
			nactvac2a integer,
			nloghvac5a integer,
			nactvac5a integer,
			nmediocre integer,
			nloghlm integer,
			nloghlls integer,
			npevd integer,
			stotd integer,
			npevp integer,
			sprincp integer,
			ssecp integer,
			ssecncp integer,
			sparkp integer,
			sparkncp integer,
			slocal integer,
			tpevdom_s character varying(14) COLLATE pg_catalog."default",
			nlot integer,
			pdlmp character varying(1) COLLATE pg_catalog."default",
			ctpdl character varying(5) COLLATE pg_catalog."default",
			typecopro2 character varying(1) COLLATE pg_catalog."default",
			ncp integer,
			ndroit integer,
			ndroitindi integer,
			ndroitpro integer,
			ndroitges integer,
			catpro2 character varying(20) COLLATE pg_catalog."default",
			catpro2txt character varying(200) COLLATE pg_catalog."default",
			catpro3 character varying(30) COLLATE pg_catalog."default",
			catpropro2 character varying(20) COLLATE pg_catalog."default",
			catproges2 character varying(30) COLLATE pg_catalog."default",
			locprop character varying(1) COLLATE pg_catalog."default",
			locproptxt character varying(21) COLLATE pg_catalog."default",
			geompar geometry(MultiPolygon,2154),
			geomloc geometry(Point,2154),
			source_geo character varying(34) COLLATE pg_catalog."default",
			vecteur character varying(1) COLLATE pg_catalog."default",
			contour character varying(3) COLLATE pg_catalog."default",
			idpk integer NOT NULL,
			--SUITE A CONSERVER
			a_prop_cod_prop bpchar[] COLLATE pg_catalog."default",
			a_prop_id bigint[],
			a_prop_idu text[] COLLATE pg_catalog."default",
			a_prop_public boolean[],
			a_prop_cominterco boolean[],
			a_prop_com_niv character varying[] COLLATE pg_catalog."default",
			a_prop_com_niv_libelle text[] COLLATE pg_catalog."default",
			a_ges_cod_prop bpchar[] COLLATE pg_catalog."default",
			a_ges_id bigint[],
			a_ges_idu text[] COLLATE pg_catalog."default",
			a_ges_public boolean[],
			a_ges_cominterco boolean[],
			a_ges_com_niv character varying[] COLLATE pg_catalog."default",
			a_ges_com_niv_libelle text[] COLLATE pg_catalog."default",
			cominterco boolean,
			com_niv int,
			idcom22 character varying(5) COLLATE pg_catalog."default",
			PRIMARY KEY(ccodep,idpk),
			UNIQUE(ccodep,idpar)
		)
		PARTITION BY LIST(ccodep);
		
		--CREATION D'UNE TABLE TEMPORAIRE DES DEPARTEMENTS
		CREATE SCHEMA IF NOT EXISTS temp;
		DROP TABLE IF EXISTS temp.dep;
		CREATE TABLE temp.dep AS (
			WITH sub AS (
				SELECT par.ccodep
				FROM %I.%I par
				GROUP BY par.ccodep)
			SELECT ARRAY_AGG(ccodep) ccodep
			FROM sub
		);		
		$$, table_name_new, table_name_new, schema_name, table_name_parcelle);

		--Création des partitions par département
		a_ccodep = (SELECT dep.ccodep FROM temp.dep dep);

		FOREACH ccodep IN ARRAY ARRAY[a_ccodep]
		LOOP
			partition_tablename = table_name_new||'_'||ccodep;
		EXECUTE format(
		$$ 
				CREATE TABLE IF NOT EXISTS nat.%I
				PARTITION OF nat.%I
				FOR VALUES IN (%L)
			$$, partition_tablename,table_name_new,ccodep);
		END LOOP;
		
		EXECUTE format(
		$$
			DROP TABLE temp.dep
		$$);
		
END
$do$


---2020 (38s)---

DO
$do$
DECLARE
	millesime text;
	millesime_short text;
	ccodep text;
  	partition_tablename text;
  	a_ccodep text[];
	table_temp_dep text;
	schema_name text;
	table_name_parcelle text;
	table_name_new text;
BEGIN
	millesime = '2020';
	millesime_short = RIGHT(millesime,2);
	table_temp_dep = 'dep'||millesime;
	schema_name = 'ff_'||millesime;
	table_name_parcelle = 'fftp_'||millesime||'_pnb10_parcelle';
	table_name_new = 'public'||millesime_short;
	
	EXECUTE format(
		$$
		--Création de la table des parcelles
		DROP TABLE IF EXISTS nat.%I CASCADE;
		CREATE TABLE IF NOT EXISTS nat.%I(
			idpar character varying(14) COLLATE pg_catalog."default",
			idsec character varying(10) COLLATE pg_catalog."default",
			idprocpte character varying(11) COLLATE pg_catalog."default",
			idparref character varying(14) COLLATE pg_catalog."default",
			idsecref character varying(10) COLLATE pg_catalog."default",
			idvoie character varying(9) COLLATE pg_catalog."default",
			idcom character varying(5) COLLATE pg_catalog."default",
			idcomtxt character varying(45) COLLATE pg_catalog."default",
			ccodep character varying(2) COLLATE pg_catalog."default",
			ccodir character varying(1) COLLATE pg_catalog."default",
			ccocom character varying(3) COLLATE pg_catalog."default",
			ccopre character varying(3) COLLATE pg_catalog."default",
			ccosec character varying(2) COLLATE pg_catalog."default",
			dnupla character varying(4) COLLATE pg_catalog."default",
			dcntpa integer,
			dnupro character varying(6) COLLATE pg_catalog."default",
			jdatat character varying(8) COLLATE pg_catalog."default",
			jdatatv character varying(8) COLLATE pg_catalog."default",
			dreflf character varying(5) COLLATE pg_catalog."default",
			gpdl character varying(1) COLLATE pg_catalog."default",
			cprsecr character varying(3) COLLATE pg_catalog."default",
			ccosecr character varying(2) COLLATE pg_catalog."default",
			dnuplar character varying(4) COLLATE pg_catalog."default",
			dnupdl character varying(3) COLLATE pg_catalog."default",
			gurbpa character varying(1) COLLATE pg_catalog."default",
			dparpi character varying(4) COLLATE pg_catalog."default",
			ccoarp character varying(1) COLLATE pg_catalog."default",
			gparnf character varying(1) COLLATE pg_catalog."default",
			gparbat character varying(1) COLLATE pg_catalog."default",
			dnuvoi character varying(4) COLLATE pg_catalog."default",
			dindic character varying(1) COLLATE pg_catalog."default",
			ccovoi character varying(5) COLLATE pg_catalog."default",
			ccoriv character varying(4) COLLATE pg_catalog."default",
			ccocif character varying(4) COLLATE pg_catalog."default",
			ccpper character varying(3) COLLATE pg_catalog."default",
			cconvo character varying(4) COLLATE pg_catalog."default",
			dvoilib character varying(26) COLLATE pg_catalog."default",
			idparm character varying(14) COLLATE pg_catalog."default",
			ccocomm integer,
			ccoprem character varying(3) COLLATE pg_catalog."default",
			ccosecm character varying(2) COLLATE pg_catalog."default",
			dnuplam character varying(4) COLLATE pg_catalog."default",
			type character varying(1) COLLATE pg_catalog."default",
			typetxt character varying(16) COLLATE pg_catalog."default",
			ccoifp integer,
			jdatatan integer,
			jannatmin integer,
			jannatmax integer,
			jannatminh integer,
			jannatmaxh integer,
			janbilmin integer,
			nsuf integer,
			ssuf integer,
			cgrnumd character varying(2) COLLATE pg_catalog."default",
			cgrnumdtxt character varying(19) COLLATE pg_catalog."default",
			dcntsfd integer,
			dcntarti integer,
			dcntnaf double precision,
			dcnt01 integer,
			dcnt02 integer,
			dcnt03 integer,
			dcnt04 integer,
			dcnt05 integer,
			dcnt06 integer,
			dcnt07 integer,
			dcnt08 integer,
			dcnt09 integer,
			dcnt10 integer,
			dcnt11 integer,
			dcnt12 integer,
			dcnt13 integer,
			schemrem integer,
			nlocal integer,
			nlocmaison integer,
			nlocappt integer,
			nloclog integer,
			nloccom integer,
			nloccomrdc integer,
			nloccomter integer,
			ncomtersd integer,
			ncomterdep integer,
			nloccomsec integer,
			nlocdep integer,
			nlocburx integer,
			tlocdomin character varying(11) COLLATE pg_catalog."default",
			nbat integer,
			nlochab integer,
			nlogh integer,
			npevph integer,
			stoth integer,
			stotdsueic integer,
			nloghvac integer,
			nloghmeu integer,
			nloghloue integer,
			nloghpp integer,
			nloghautre integer,
			nloghnonh integer,
			nactvacant integer,
			nloghvac2a integer,
			nactvac2a integer,
			nloghvac5a integer,
			nactvac5a integer,
			nmediocre integer,
			nloghlm integer,
			nloghlls integer,
			npevd integer,
			stotd integer,
			npevp integer,
			sprincp integer,
			ssecp integer,
			ssecncp integer,
			sparkp integer,
			sparkncp integer,
			slocal integer,
			tpevdom_s character varying(14) COLLATE pg_catalog."default",
			nlot integer,
			pdlmp character varying(1) COLLATE pg_catalog."default",
			ctpdl character varying(5) COLLATE pg_catalog."default",
			typecopro2 character varying(1) COLLATE pg_catalog."default",
			ncp integer,
			ndroit integer,
			ndroitindi integer,
			ndroitpro integer,
			ndroitges integer,
			catpro2 character varying(20) COLLATE pg_catalog."default",
			catpro2txt character varying(200) COLLATE pg_catalog."default",
			catpro3 character varying(30) COLLATE pg_catalog."default",
			catpropro2 character varying(20) COLLATE pg_catalog."default",
			catproges2 character varying(30) COLLATE pg_catalog."default",
			locprop character varying(1) COLLATE pg_catalog."default",
			locproptxt character varying(21) COLLATE pg_catalog."default",
			geompar geometry(MultiPolygon,2154),
			geomloc geometry(Point,2154),
			source_geo character varying(34) COLLATE pg_catalog."default",
			vecteur character varying(1) COLLATE pg_catalog."default",
			contour character varying(3) COLLATE pg_catalog."default",
			idpk integer NOT NULL,
			--SUITE A CONSERVER
			a_prop_cod_prop bpchar[] COLLATE pg_catalog."default",
			a_prop_id bigint[],
			a_prop_idu text[] COLLATE pg_catalog."default",
			a_prop_public boolean[],
			a_prop_cominterco boolean[],
			a_prop_com_niv character varying[] COLLATE pg_catalog."default",
			a_prop_com_niv_libelle text[] COLLATE pg_catalog."default",
			a_ges_cod_prop bpchar[] COLLATE pg_catalog."default",
			a_ges_id bigint[],
			a_ges_idu text[] COLLATE pg_catalog."default",
			a_ges_public boolean[],
			a_ges_cominterco boolean[],
			a_ges_com_niv character varying[] COLLATE pg_catalog."default",
			a_ges_com_niv_libelle text[] COLLATE pg_catalog."default",
			cominterco boolean,
			com_niv int,
			idcom22 character varying(5) COLLATE pg_catalog."default",
			PRIMARY KEY(ccodep,idpk),
			UNIQUE(ccodep,idpar)
		)
		PARTITION BY LIST(ccodep);
		
		--CREATION D'UNE TABLE TEMPORAIRE DES DEPARTEMENTS
		CREATE SCHEMA IF NOT EXISTS temp;
		DROP TABLE IF EXISTS temp.dep;
		CREATE TABLE temp.dep AS (
			WITH sub AS (
				SELECT par.ccodep
				FROM %I.%I par
				GROUP BY par.ccodep)
			SELECT ARRAY_AGG(ccodep) ccodep
			FROM sub
		);		
		$$, table_name_new, table_name_new, schema_name, table_name_parcelle);

		--Création des partitions par département
		a_ccodep = (SELECT dep.ccodep FROM temp.dep dep);

		FOREACH ccodep IN ARRAY ARRAY[a_ccodep]
		LOOP
			partition_tablename = table_name_new||'_'||ccodep;
		EXECUTE format(
		$$ 
				CREATE TABLE IF NOT EXISTS nat.%I
				PARTITION OF nat.%I
				FOR VALUES IN (%L)
			$$, partition_tablename,table_name_new,ccodep);
		END LOOP;
		
		EXECUTE format(
		$$
			DROP TABLE temp.dep
		$$);
		
END
$do$


---2021 (40s)---

DO
$do$
DECLARE
	millesime text;
	millesime_short text;
	ccodep text;
  	partition_tablename text;
  	a_ccodep text[];
	table_temp_dep text;
	schema_name text;
	table_name_parcelle text;
	table_name_new text;
BEGIN
	millesime = '2021';
	millesime_short = RIGHT(millesime,2);
	table_temp_dep = 'dep'||millesime;
	schema_name = 'ff_'||millesime;
	table_name_parcelle = 'fftp_'||millesime||'_pnb10_parcelle';
	table_name_new = 'public'||millesime_short;
	
	EXECUTE format(
		$$
		--Création de la table des parcelles
		DROP TABLE IF EXISTS nat.%I CASCADE;
		CREATE TABLE IF NOT EXISTS nat.%I(
			idpar character varying(14) COLLATE pg_catalog."default",
			idtup character varying COLLATE pg_catalog."default",
			idsec character varying(10) COLLATE pg_catalog."default",
			idprocpte character varying(11) COLLATE pg_catalog."default",
			idparref character varying(14) COLLATE pg_catalog."default",
			idsecref character varying(10) COLLATE pg_catalog."default",
			idvoie character varying(9) COLLATE pg_catalog."default",
			idcom character varying(5) COLLATE pg_catalog."default",
			idcomtxt character varying(45) COLLATE pg_catalog."default",
			ccodep character varying(2) COLLATE pg_catalog."default",
			ccodir character varying(1) COLLATE pg_catalog."default",
			ccocom character varying(3) COLLATE pg_catalog."default",
			ccopre character varying(3) COLLATE pg_catalog."default",
			ccosec character varying(2) COLLATE pg_catalog."default",
			dnupla character varying(4) COLLATE pg_catalog."default",
			dcntpa integer,
			dnupro character varying(6) COLLATE pg_catalog."default",
			jdatat character varying(8) COLLATE pg_catalog."default",
			jdatatv character varying(8) COLLATE pg_catalog."default",
			dreflf character varying(5) COLLATE pg_catalog."default",
			gpdl character varying(1) COLLATE pg_catalog."default",
			cprsecr character varying(3) COLLATE pg_catalog."default",
			ccosecr character varying(2) COLLATE pg_catalog."default",
			dnuplar character varying(4) COLLATE pg_catalog."default",
			dnupdl character varying(3) COLLATE pg_catalog."default",
			gurbpa character varying(1) COLLATE pg_catalog."default",
			dparpi character varying(4) COLLATE pg_catalog."default",
			ccoarp character varying(1) COLLATE pg_catalog."default",
			gparnf character varying(1) COLLATE pg_catalog."default",
			gparbat character varying(1) COLLATE pg_catalog."default",
			dnuvoi character varying(4) COLLATE pg_catalog."default",
			dindic character varying(1) COLLATE pg_catalog."default",
			ccovoi character varying(5) COLLATE pg_catalog."default",
			ccoriv character varying(4) COLLATE pg_catalog."default",
			ccocif character varying(4) COLLATE pg_catalog."default",
			ccpper character varying(3) COLLATE pg_catalog."default",
			cconvo character varying(4) COLLATE pg_catalog."default",
			dvoilib character varying(26) COLLATE pg_catalog."default",
			idparm character varying(14) COLLATE pg_catalog."default",
			ccocomm integer,
			ccoprem character varying(3) COLLATE pg_catalog."default",
			ccosecm character varying(2) COLLATE pg_catalog."default",
			dnuplam character varying(4) COLLATE pg_catalog."default",
			type character varying(1) COLLATE pg_catalog."default",
			typetxt character varying(16) COLLATE pg_catalog."default",
			ccoifp integer,
			jdatatan integer,
			jannatmin integer,
			jannatmax integer,
			jannatminh integer,
			jannatmaxh integer,
			janbilmin integer,
			nsuf integer,
			ssuf integer,
			cgrnumd character varying(2) COLLATE pg_catalog."default",
			cgrnumdtxt character varying(19) COLLATE pg_catalog."default",
			dcntsfd integer,
			dcntarti integer,
			dcntnaf integer,
			dcnt01 integer,
			dcnt02 integer,
			dcnt03 integer,
			dcnt04 integer,
			dcnt05 integer,
			dcnt06 integer,
			dcnt07 integer,
			dcnt08 integer,
			dcnt09 integer,
			dcnt10 integer,
			dcnt11 integer,
			dcnt12 integer,
			dcnt13 integer,
			schemrem integer,
			nlocal integer,
			nlocmaison integer,
			nlocappt integer,
			nloclog integer,
			nloccom integer,
			nloccomrdc integer,
			nloccomter integer,
			ncomtersd integer,
			ncomterdep integer,
			nloccomsec integer,
			nlocdep integer,
			nlocburx integer,
			tlocdomin character varying(11) COLLATE pg_catalog."default",
			nbat integer,
			nlochab integer,
			nlogh integer,
			nloghmais integer,
			nloghappt integer,
			npevph integer,
			stoth integer,
			stotdsueic integer,
			nloghvac integer,
			nloghmeu integer,
			nloghloue integer,
			nloghpp integer,
			nloghautre integer,
			nloghnonh integer,
			nactvacant integer,
			nloghvac2a integer,
			nactvac2a integer,
			nloghvac5a integer,
			nactvac5a integer,
			nmediocre integer,
			nloghlm integer,
			nloghlls integer,
			npevd integer,
			stotd integer,
			npevp integer,
			sprincp integer,
			ssecp integer,
			ssecncp integer,
			sparkp integer,
			sparkncp integer,
			slocal integer,
			tpevdom_s character varying(14) COLLATE pg_catalog."default",
			nlot integer,
			pdlmp character varying(1) COLLATE pg_catalog."default",
			ctpdl character varying(5) COLLATE pg_catalog."default",
			typecopro2 character varying(1) COLLATE pg_catalog."default",
			ncp integer,
			ndroit integer,
			ndroitindi integer,
			ndroitpro integer,
			ndroitges integer,
			catpro2 character varying(20) COLLATE pg_catalog."default",
			catpro2txt character varying(200) COLLATE pg_catalog."default",
			catpro3 character varying(30) COLLATE pg_catalog."default",
			catpropro2 character varying(20) COLLATE pg_catalog."default",
			catproges2 character varying(30) COLLATE pg_catalog."default",
			locprop character varying(1) COLLATE pg_catalog."default",
			locproptxt character varying(21) COLLATE pg_catalog."default",
			geompar geometry(MultiPolygon,2154),
			geomloc geometry(Point,2154),
			source_geo character varying(34) COLLATE pg_catalog."default",
			vecteur character varying(1) COLLATE pg_catalog."default",
			contour character varying(3) COLLATE pg_catalog."default",
			idpk integer NOT NULL,
			---SUITE A CONSERVER
			a_prop_cod_prop bpchar[] COLLATE pg_catalog."default",
			a_prop_id bigint[],
			a_prop_idu text[] COLLATE pg_catalog."default",
			a_prop_public boolean[],
			a_prop_cominterco boolean[],
			a_prop_com_niv character varying[] COLLATE pg_catalog."default",
			a_prop_com_niv_libelle text[] COLLATE pg_catalog."default",
			a_ges_cod_prop bpchar[] COLLATE pg_catalog."default",
			a_ges_id bigint[],
			a_ges_idu text[] COLLATE pg_catalog."default",
			a_ges_public boolean[],
			a_ges_cominterco boolean[],
			a_ges_com_niv character varying[] COLLATE pg_catalog."default",
			a_ges_com_niv_libelle text[] COLLATE pg_catalog."default",
			cominterco boolean,
			com_niv int,
			idcom22 character varying(5) COLLATE pg_catalog."default",
			PRIMARY KEY(ccodep,idpk),
			UNIQUE(ccodep,idpar)
		)
		PARTITION BY LIST(ccodep);
		
		--CREATION D'UNE TABLE TEMPORAIRE DES DEPARTEMENTS
		CREATE SCHEMA IF NOT EXISTS temp;
		DROP TABLE IF EXISTS temp.dep;
		CREATE TABLE temp.dep AS (
			WITH sub AS (
				SELECT par.ccodep
				FROM %I.%I par
				GROUP BY par.ccodep)
			SELECT ARRAY_AGG(ccodep) ccodep
			FROM sub
		);		
		$$, table_name_new, table_name_new, schema_name, table_name_parcelle);

		--Création des partitions par département
		a_ccodep = (SELECT dep.ccodep FROM temp.dep dep);

		FOREACH ccodep IN ARRAY ARRAY[a_ccodep]
		LOOP
			partition_tablename = table_name_new||'_'||ccodep;
		EXECUTE format(
		$$ 
				CREATE TABLE IF NOT EXISTS nat.%I
				PARTITION OF nat.%I
				FOR VALUES IN (%L)
			$$, partition_tablename,table_name_new,ccodep);
		END LOOP;
		
		EXECUTE format(
		$$
			DROP TABLE temp.dep
		$$);
		
END
$do$




------------------------------------------
----EXTRACTION ET IMPORT DES DONNEES------
------------------------------------------

DO
\$do\$
DECLARE
	millesime text;
	millesime_short text;
	schema_name text;
	table_name_parcelle text;
	table_name_prop text;
	table_name_new text;
	typedroit_var text;
	typpro text;
BEGIN
	CREATE SCHEMA IF NOT EXISTS nat;
	
	RAISE NOTICE 'Import multi-millésimes démarré';
	
	FOREACH millesime IN ARRAY ARRAY['2009','2011','2012','2013','2014','2015','2016','2017','2018','2019','2020','2021']
	LOOP
		RAISE NOTICE 'Import du millésime % en cours',millesime;
		millesime_short = RIGHT(millesime,2);
		schema_name = 'ff_'||millesime;
		table_name_parcelle = 'fftp_'||millesime||'_pnb10_parcelle';
		table_name_prop = 'fftp_'||millesime||'_proprietaire_droit';
		table_name_new = 'public'||millesime_short;
		
		
	IF millesime LIKE '2009' THEN	
		typedroit_var = 'ccodro';

		EXECUTE format(
		\$\$
		--Extraction idpar à au moins 1 propriétaire public (1m37)
		WITH public_par AS (
			SELECT DISTINCT par.*
			FROM %I.%I par
			JOIN %I.%I prop1 ON par.ccodep = prop1.ccodep
				AND par.idprocpte = prop1.idprocpte
			JOIN prop_mm.proprio_final_m2021_work prop2 ON prop1.idprodroit = prop2.idprodroit
			WHERE prop1.%I ~ '^[BCFNPVX]$' AND prop2.public IS TRUE),

		--Extraction de tous les propriétaires et gestionnaires de ces parcelles (25s)
		public_prop AS (
			SELECT DISTINCT par.idpar, prop1.idprodroit,
				CASE
					WHEN prop1.%I ~ '^[BCFNPVX]$' THEN 'P'
					ELSE 'G'
				END AS typedroit
			FROM public_par par
			JOIN %I.%I prop1 ON par.ccodep = prop1.ccodep
				AND par.idprocpte = prop1.idprocpte),

		---Extraction des infos propriétaires et gestionnaires personnes morales (16s)
		public_prop2 AS (
			SELECT prop1.idpar, prop1.typedroit,
				prop2.id AS idprop, prop2.idu, COALESCE(prop2.cod_prop,'X1') cod_prop,
				COALESCE(prop2.public,'False') public, COALESCE(prop2.cominterco, 'False') cominterco,
				prop2.com_niv, prop2.com_niv_libelle
			FROM public_prop prop1
			LEFT JOIN prop_mm.proprio_final_m2021_work prop2 ON prop1.idprodroit = prop2.idprodroit)

		---Extraction des parcelles à au moins 1 propriétaire public + aggrégation des propriétaires(2m03) 
		INSERT INTO nat.%I
			SELECT par.idpar, idsec, idprocpte, idparref, idsecref, idvoie, idcom, idcomtxt, ccodep, ccodir, ccocom, ccopre, ccosec,
			dnupla, dcntpa, dsrpar, dnupro, jdatat, dreflf, gpdl, cprsecr, ccosecr, dnuplar, dnupdl, gurbpa, dparpi, ccoarp, gparnf,
			gparbat, dnvoiri, dindic, ccovoi, ccoriv, ccocif, jdatatan, jannatmin, jannatmax, janbilmin, nsuf, ssuf, cgrnumd, cgrnumdtxt,
			dcntsfd, dcntsol, dcntagri, dcntbois, dcntnat, dcnteau, dcnt01, dcnt02, dcnt03, dcnt04, dcnt06, dcnt07, dcnt09, dcnt10,
			dcnt11, dcnt12, dcnt13, schemrem, nlocal, nlocmaison, nlocappt, nlochabit, nloccom, nloccomrdc, nlocdep, tlocdomin, nbat,
			npevaffh, npevph, stoth, smoyh, npiecemoy, stotdsueic, nvacant, nmediocre, nloghlm, noccprop, nocclocat, typoocc, npevp, stotp,
			smoyp, npevd, stotd, smoyd, spevtot, tpevdom_n, tpevdom_s, nlot, cmp, ndroit, ndroitindi, ndroitpro, ndroitges, typprop,
			typproppro, typpropges, geomloc, geompar, source_geo, vecteur, idpk,
			ARRAY_AGG(prop2.cod_prop) FILTER (WHERE typedroit LIKE 'P') as a_prop_cod_prop,
			ARRAY_AGG(prop2.idprop) FILTER (WHERE typedroit LIKE 'P') as a_prop_id,
			ARRAY_AGG(prop2.idu) FILTER (WHERE typedroit LIKE 'P') as a_prop_idu,
			ARRAY_AGG(prop2.public) FILTER (WHERE typedroit LIKE 'P') as a_prop_public,
			ARRAY_AGG(prop2.cominterco) FILTER (WHERE typedroit LIKE 'P') as a_prop_cominterco,
			ARRAY_AGG(prop2.com_niv) FILTER (WHERE typedroit LIKE 'P') as a_prop_com_niv,
			ARRAY_AGG(prop2.com_niv_libelle) FILTER (WHERE typedroit LIKE 'P') as a_prop_com_niv_libelle,
			ARRAY_AGG(prop2.cod_prop) FILTER (WHERE typedroit LIKE 'G') as a_ges_cod_prop,
			ARRAY_AGG(prop2.idprop) FILTER (WHERE typedroit LIKE 'G') as a_ges_id,
			ARRAY_AGG(prop2.idu) FILTER (WHERE typedroit LIKE 'G') as a_ges_idu,
			ARRAY_AGG(prop2.public) FILTER (WHERE typedroit LIKE 'G') as a_ges_public,
			ARRAY_AGG(prop2.cominterco) FILTER (WHERE typedroit LIKE 'G') as a_ges_cominterco,
			ARRAY_AGG(prop2.com_niv) FILTER (WHERE typedroit LIKE 'G') as a_ges_com_niv,
			ARRAY_AGG(prop2.com_niv_libelle) FILTER (WHERE typedroit LIKE 'G') as a_ges_com_niv_libelle,
			Null AS idcom22
		FROM public_par par
		JOIN public_prop2 prop2 ON par.idpar = prop2.idpar
		GROUP BY par.idpar, idsec, idprocpte, idparref, idsecref, idvoie, idcom, idcomtxt, ccodep, ccodir, ccocom, ccopre, ccosec,
			dnupla, dcntpa, dsrpar, dnupro, jdatat, dreflf, gpdl, cprsecr, ccosecr, dnuplar, dnupdl, gurbpa, dparpi, ccoarp, gparnf,
			gparbat, dnvoiri, dindic, ccovoi, ccoriv, ccocif, jdatatan, jannatmin, jannatmax, janbilmin, nsuf, ssuf, cgrnumd, cgrnumdtxt,
			dcntsfd, dcntsol, dcntagri, dcntbois, dcntnat, dcnteau, dcnt01, dcnt02, dcnt03, dcnt04, dcnt06, dcnt07, dcnt09, dcnt10,
			dcnt11, dcnt12, dcnt13, schemrem, nlocal, nlocmaison, nlocappt, nlochabit, nloccom, nloccomrdc, nlocdep, tlocdomin, nbat,
			npevaffh, npevph, stoth, smoyh, npiecemoy, stotdsueic, nvacant, nmediocre, nloghlm, noccprop, nocclocat, typoocc, npevp, stotp,
			smoyp, npevd, stotd, smoyd, spevtot, tpevdom_n, tpevdom_s, nlot, cmp, ndroit, ndroitindi, ndroitpro, ndroitges, typprop,
			typproppro, typpropges, geomloc, geompar, source_geo, vecteur, idpk;

		\$\$, 
			--public_par
			schema_name, table_name_parcelle,
			schema_name, table_name_prop,
			typedroit_var,
			--public_prop
			typedroit_var,
			schema_name, table_name_prop,
			--nat.public_XX
			table_name_new);	


	ELSIF millesime LIKE '2011' THEN	
		typedroit_var = 'ccodro';

		EXECUTE format(
		\$\$
		--Extraction idpar à au moins 1 propriétaire public (1m37)
		WITH public_par AS (
			SELECT DISTINCT par.*
			FROM %I.%I par
			JOIN %I.%I prop1 ON par.ccodep = prop1.ccodep
				AND par.idprocpte = prop1.idprocpte
			JOIN prop_mm.proprio_final_m2021_work prop2 ON prop1.idprodroit = prop2.idprodroit
			WHERE prop1.%I ~ '^[BCFNPVX]$' AND prop2.public IS TRUE),

		--Extraction de tous les propriétaires et gestionnaires de ces parcelles (25s)
		public_prop AS (
			SELECT DISTINCT par.idpar, prop1.idprodroit,
				CASE
					WHEN prop1.%I ~ '^[BCFNPVX]$' THEN 'P'
					ELSE 'G'
				END AS typedroit
			FROM public_par par
			JOIN %I.%I prop1 ON par.ccodep = prop1.ccodep
				AND par.idprocpte = prop1.idprocpte),

		---Extraction des infos propriétaires et gestionnaires personnes morales (16s)
		public_prop2 AS (
			SELECT prop1.idpar, prop1.typedroit,
				prop2.id AS idprop, prop2.idu, COALESCE(prop2.cod_prop,'X1') cod_prop,
				COALESCE(prop2.public,'False') public, COALESCE(prop2.cominterco, 'False') cominterco,
				prop2.com_niv, prop2.com_niv_libelle
			FROM public_prop prop1
			LEFT JOIN prop_mm.proprio_final_m2021_work prop2 ON prop1.idprodroit = prop2.idprodroit)

		---Extraction des parcelles à au moins 1 propriétaire public + aggrégation des propriétaires(2m03) 
		INSERT INTO nat.%I
			SELECT par.idpar, idsec, idprocpte, idparref, idsecref, idvoie, idcom, idcomtxt, ccodep, ccodir, ccocom, ccopre, ccosec, dnupla,
			dcntpa, dsrpar, dnupro, jdatat, dreflf, gpdl, cprsecr, ccosecr, dnuplar, dnupdl, gurbpa, dparpi, ccoarp, gparnf, gparbat,
			dnvoiri, dindic, ccovoi, ccoriv, ccocif, jdatatan, jannatmin, jannatmax, janbilmin, nsuf, ssuf, cgrnumd, cgrnumdtxt, dcntsfd,
			dcntsol, dcntagri, dcntbois, dcntnat, dcnteau, dcnt01, dcnt02, dcnt03, dcnt04, dcnt06, dcnt07, dcnt09, dcnt10, dcnt11, dcnt12,
			dcnt13, schemrem, nlocal, nlocmaison, nlocappt, nlochabit, nloccom, nloccomrdc, nlocdep, nlocburx, tlocdomin, nbat, npevaffh,
			npevph, stoth, smoyh, npiecemoy, stotdsueic, nvacant, nmediocre, nloghlm, noccprop, nocclocat, typoocc, npevp, stotp, smoyp,
			npevd, stotd, smoyd, spevtot, tpevdom_n, tpevdom_s, nlot, cmp, ndroit, ndroitindi, ndroitpro, ndroitges, typprop, typproppro,
			typpropges, geomloc, geompar, source_geo, vecteur, idpk,
			ARRAY_AGG(prop2.cod_prop) FILTER (WHERE typedroit LIKE 'P') as a_prop_cod_prop,
			ARRAY_AGG(prop2.idprop) FILTER (WHERE typedroit LIKE 'P') as a_prop_id,
			ARRAY_AGG(prop2.idu) FILTER (WHERE typedroit LIKE 'P') as a_prop_idu,
			ARRAY_AGG(prop2.public) FILTER (WHERE typedroit LIKE 'P') as a_prop_public,
			ARRAY_AGG(prop2.cominterco) FILTER (WHERE typedroit LIKE 'P') as a_prop_cominterco,
			ARRAY_AGG(prop2.com_niv) FILTER (WHERE typedroit LIKE 'P') as a_prop_com_niv,
			ARRAY_AGG(prop2.com_niv_libelle) FILTER (WHERE typedroit LIKE 'P') as a_prop_com_niv_libelle,
			ARRAY_AGG(prop2.cod_prop) FILTER (WHERE typedroit LIKE 'G') as a_ges_cod_prop,
			ARRAY_AGG(prop2.idprop) FILTER (WHERE typedroit LIKE 'G') as a_ges_id,
			ARRAY_AGG(prop2.idu) FILTER (WHERE typedroit LIKE 'G') as a_ges_idu,
			ARRAY_AGG(prop2.public) FILTER (WHERE typedroit LIKE 'G') as a_ges_public,
			ARRAY_AGG(prop2.cominterco) FILTER (WHERE typedroit LIKE 'G') as a_ges_cominterco,
			ARRAY_AGG(prop2.com_niv) FILTER (WHERE typedroit LIKE 'G') as a_ges_com_niv,
			ARRAY_AGG(prop2.com_niv_libelle) FILTER (WHERE typedroit LIKE 'G') as a_ges_com_niv_libelle,
			Null AS idcom22
		FROM public_par par
		JOIN public_prop2 prop2 ON par.idpar = prop2.idpar
		GROUP BY par.idpar, idsec, idprocpte, idparref, idsecref, idvoie, idcom, idcomtxt, ccodep, ccodir, ccocom, ccopre, ccosec, dnupla,
			dcntpa, dsrpar, dnupro, jdatat, dreflf, gpdl, cprsecr, ccosecr, dnuplar, dnupdl, gurbpa, dparpi, ccoarp, gparnf, gparbat,
			dnvoiri, dindic, ccovoi, ccoriv, ccocif, jdatatan, jannatmin, jannatmax, janbilmin, nsuf, ssuf, cgrnumd, cgrnumdtxt, dcntsfd,
			dcntsol, dcntagri, dcntbois, dcntnat, dcnteau, dcnt01, dcnt02, dcnt03, dcnt04, dcnt06, dcnt07, dcnt09, dcnt10, dcnt11, dcnt12,
			dcnt13, schemrem, nlocal, nlocmaison, nlocappt, nlochabit, nloccom, nloccomrdc, nlocdep, nlocburx, tlocdomin, nbat, npevaffh,
			npevph, stoth, smoyh, npiecemoy, stotdsueic, nvacant, nmediocre, nloghlm, noccprop, nocclocat, typoocc, npevp, stotp, smoyp,
			npevd, stotd, smoyd, spevtot, tpevdom_n, tpevdom_s, nlot, cmp, ndroit, ndroitindi, ndroitpro, ndroitges, typprop, typproppro,
			typpropges, geomloc, geompar, source_geo, vecteur, idpk;

		\$\$, 
			--public_par
			schema_name, table_name_parcelle,
			schema_name, table_name_prop,
			typedroit_var,
			--public_prop
			typedroit_var,
			schema_name, table_name_prop,
			--nat.public_XX
			table_name_new);
			
	
	ELSIF millesime LIKE '2012' THEN	
		typedroit_var = 'ccodro';

		EXECUTE format(
		\$\$
		--Extraction idpar à au moins 1 propriétaire public (1m37)
		WITH public_par AS (
			SELECT DISTINCT par.*
			FROM %I.%I par
			JOIN %I.%I prop1 ON par.ccodep = prop1.ccodep
				AND par.idprocpte = prop1.idprocpte
			JOIN prop_mm.proprio_final_m2021_work prop2 ON prop1.idprodroit = prop2.idprodroit
			WHERE prop1.%I ~ '^[BCFNPVX]$' AND prop2.public IS TRUE),

		--Extraction de tous les propriétaires et gestionnaires de ces parcelles (25s)
		public_prop AS (
			SELECT DISTINCT par.idpar, prop1.idprodroit,
				CASE
					WHEN prop1.%I ~ '^[BCFNPVX]$' THEN 'P'
					ELSE 'G'
				END AS typedroit
			FROM public_par par
			JOIN %I.%I prop1 ON par.ccodep = prop1.ccodep
				AND par.idprocpte = prop1.idprocpte),

		---Extraction des infos propriétaires et gestionnaires personnes morales (16s)
		public_prop2 AS (
			SELECT prop1.idpar, prop1.typedroit,
				prop2.id AS idprop, prop2.idu, COALESCE(prop2.cod_prop,'X1') cod_prop,
				COALESCE(prop2.public,'False') public, COALESCE(prop2.cominterco, 'False') cominterco,
				prop2.com_niv, prop2.com_niv_libelle
			FROM public_prop prop1
			LEFT JOIN prop_mm.proprio_final_m2021_work prop2 ON prop1.idprodroit = prop2.idprodroit)

		---Extraction des parcelles à au moins 1 propriétaire public + aggrégation des propriétaires(2m03) 
		INSERT INTO nat.%I
			SELECT par.idpar, idsec, idprocpte, idparref, idsecref, idvoie, idcom, idcomtxt, ccodep, ccodir, ccocom, ccopre, ccosec, dnupla,
			dcntpa, dsrpar, dnupro, jdatat, jdatatv, dreflf, gpdl, cprsecr, ccosecr, dnuplar, dnupdl, gurbpa, dparpi, ccoarp, gparnf,
			gparbat, dnvoiri, dindic, ccovoi, ccoriv, ccocif, cconvo, dvoilib, idparm, ccocomm, ccoprem, ccosecm, dnuplam, type, typetxt,
			jdatatan, jannatmin, jannatmax, jannatminh, jannatmaxh, janbilmin, nsuf, ssuf, cgrnumd, cgrnumdtxt, dcntsfd, dcntsol, dcntagri,
			dcntbois, dcntnat, dcnteau, dcnt01, dcnt02, dcnt03, dcnt04, dcnt06, dcnt07, dcnt09, dcnt10, dcnt11, dcnt12, dcnt13, schemrem,
			nlocal, nlocmaison, nlocappt, nlochabit, nloccom, nloccomrdc, nloccomter, nloccomsec, nlocdep, nlocburx, tlocdomin, nbat,
			npevaffh, npevph, stoth, smoyh, npiecemoy, stotdsueic, nhabvacant, nactvacant, nmediocre, nloghlm, noccprop, nocclocat, typoocc,
			npevp, stotp, smoyp, npevd, stotd, smoyd, spevtot, tpevdom_n, tpevdom_s, nlot, pdlmp, ctpdl, ndroit, ndroitindi, ndroitpro,
			ndroitges, typprop, typproptxt, typproppro, typpropprotxt, typpropges, typpropgestxt, locprop, locproptxt, geomloc, geompar,
			source_geo, vecteur, idpk,
			ARRAY_AGG(prop2.cod_prop) FILTER (WHERE typedroit LIKE 'P') as a_prop_cod_prop,
			ARRAY_AGG(prop2.idprop) FILTER (WHERE typedroit LIKE 'P') as a_prop_id,
			ARRAY_AGG(prop2.idu) FILTER (WHERE typedroit LIKE 'P') as a_prop_idu,
			ARRAY_AGG(prop2.public) FILTER (WHERE typedroit LIKE 'P') as a_prop_public,
			ARRAY_AGG(prop2.cominterco) FILTER (WHERE typedroit LIKE 'P') as a_prop_cominterco,
			ARRAY_AGG(prop2.com_niv) FILTER (WHERE typedroit LIKE 'P') as a_prop_com_niv,
			ARRAY_AGG(prop2.com_niv_libelle) FILTER (WHERE typedroit LIKE 'P') as a_prop_com_niv_libelle,
			ARRAY_AGG(prop2.cod_prop) FILTER (WHERE typedroit LIKE 'G') as a_ges_cod_prop,
			ARRAY_AGG(prop2.idprop) FILTER (WHERE typedroit LIKE 'G') as a_ges_id,
			ARRAY_AGG(prop2.idu) FILTER (WHERE typedroit LIKE 'G') as a_ges_idu,
			ARRAY_AGG(prop2.public) FILTER (WHERE typedroit LIKE 'G') as a_ges_public,
			ARRAY_AGG(prop2.cominterco) FILTER (WHERE typedroit LIKE 'G') as a_ges_cominterco,
			ARRAY_AGG(prop2.com_niv) FILTER (WHERE typedroit LIKE 'G') as a_ges_com_niv,
			ARRAY_AGG(prop2.com_niv_libelle) FILTER (WHERE typedroit LIKE 'G') as a_ges_com_niv_libelle,
			Null AS idcom22
		FROM public_par par
		JOIN public_prop2 prop2 ON par.idpar = prop2.idpar
		GROUP BY par.idpar, idsec, idprocpte, idparref, idsecref, idvoie, idcom, idcomtxt, ccodep, ccodir, ccocom, ccopre, ccosec, dnupla,
			dcntpa, dsrpar, dnupro, jdatat, jdatatv, dreflf, gpdl, cprsecr, ccosecr, dnuplar, dnupdl, gurbpa, dparpi, ccoarp, gparnf,
			gparbat, dnvoiri, dindic, ccovoi, ccoriv, ccocif, cconvo, dvoilib, idparm, ccocomm, ccoprem, ccosecm, dnuplam, type, typetxt,
			jdatatan, jannatmin, jannatmax, jannatminh, jannatmaxh, janbilmin, nsuf, ssuf, cgrnumd, cgrnumdtxt, dcntsfd, dcntsol, dcntagri,
			dcntbois, dcntnat, dcnteau, dcnt01, dcnt02, dcnt03, dcnt04, dcnt06, dcnt07, dcnt09, dcnt10, dcnt11, dcnt12, dcnt13, schemrem,
			nlocal, nlocmaison, nlocappt, nlochabit, nloccom, nloccomrdc, nloccomter, nloccomsec, nlocdep, nlocburx, tlocdomin, nbat,
			npevaffh, npevph, stoth, smoyh, npiecemoy, stotdsueic, nhabvacant, nactvacant, nmediocre, nloghlm, noccprop, nocclocat, typoocc,
			npevp, stotp, smoyp, npevd, stotd, smoyd, spevtot, tpevdom_n, tpevdom_s, nlot, pdlmp, ctpdl, ndroit, ndroitindi, ndroitpro,
			ndroitges, typprop, typproptxt, typproppro, typpropprotxt, typpropges, typpropgestxt, locprop, locproptxt, geomloc, geompar,
			source_geo, vecteur, idpk;

		\$\$, 
			--public_par
			schema_name, table_name_parcelle,
			schema_name, table_name_prop,
			typedroit_var,
			--public_prop
			typedroit_var,
			schema_name, table_name_prop,
			--nat.public_XX
			table_name_new);
	
	
	ELSIF millesime LIKE '2013' THEN	
		typedroit_var = 'ccodro';

		EXECUTE format(
		\$\$
		--Extraction idpar à au moins 1 propriétaire public (1m37)
		WITH public_par AS (
			SELECT DISTINCT par.*
			FROM %I.%I par
			JOIN %I.%I prop1 ON par.ccodep = prop1.ccodep
				AND par.idprocpte = prop1.idprocpte
			JOIN prop_mm.proprio_final_m2021_work prop2 ON prop1.idprodroit = prop2.idprodroit
			WHERE prop1.%I ~ '^[BCFNPVX]$' AND prop2.public IS TRUE),

		--Extraction de tous les propriétaires et gestionnaires de ces parcelles (25s)
		public_prop AS (
			SELECT DISTINCT par.idpar, prop1.idprodroit,
				CASE
					WHEN prop1.%I ~ '^[BCFNPVX]$' THEN 'P'
					ELSE 'G'
				END AS typedroit
			FROM public_par par
			JOIN %I.%I prop1 ON par.ccodep = prop1.ccodep
				AND par.idprocpte = prop1.idprocpte),

		---Extraction des infos propriétaires et gestionnaires personnes morales (16s)
		public_prop2 AS (
			SELECT prop1.idpar, prop1.typedroit,
				prop2.id AS idprop, prop2.idu, COALESCE(prop2.cod_prop,'X1') cod_prop,
				COALESCE(prop2.public,'False') public, COALESCE(prop2.cominterco, 'False') cominterco,
				prop2.com_niv, prop2.com_niv_libelle
			FROM public_prop prop1
			LEFT JOIN prop_mm.proprio_final_m2021_work prop2 ON prop1.idprodroit = prop2.idprodroit)

		---Extraction des parcelles à au moins 1 propriétaire public + aggrégation des propriétaires(2m03) 
		INSERT INTO nat.%I
			SELECT par.idpar, idsec, idprocpte, idparref, idsecref, idvoie, idcom, idcomtxt, ccodep, ccodir, ccocom, ccopre, ccosec, dnupla,
			dcntpa, dsrpar, dnupro, jdatat, jdatatv, dreflf, gpdl, cprsecr, ccosecr, dnuplar, dnupdl, gurbpa, dparpi, ccoarp, gparnf,
			gparbat, dnvoiri, dindic, ccovoi, ccoriv, ccocif, cconvo, dvoilib, idparm, ccocomm, ccoprem, ccosecm, dnuplam, type, typetxt,
			jdatatan, jannatmin, jannatmax, jannatminh, jannatmaxh, janbilmin, nsuf, ssuf, cgrnumd, cgrnumdtxt, dcntsfd, dcntsol, dcntagri,
			dcntbois, dcntnat, dcnteau, dcnt01, dcnt02, dcnt03, dcnt04, dcnt06, dcnt07, dcnt09, dcnt10, dcnt11, dcnt12, dcnt13, schemrem,
			nlocal, nlocmaison, nlocappt, nlochabit, nloccom, nloccomrdc, nloccomter, nloccomsec, nlocdep, nlocburx, tlocdomin, nbat,
			npevaffh, npevph, stoth, smoyh, npiecemoy, stotdsueic, nhabvacant, nactvacant, nmediocre, nloghlm, noccprop, nocclocat, typoocc,
			npevp, stotp, smoyp, npevd, stotd, smoyd, spevtot, tpevdom_n, tpevdom_s, nlot, pdlmp, ctpdl, ndroit, ndroitindi, ndroitpro,
			ndroitges, typprop, typproptxt, typproppro, typpropprotxt, typpropges, typpropgestxt, locprop, locproptxt, geomloc, geompar,
			source_geo, vecteur, idpk,
			ARRAY_AGG(prop2.cod_prop) FILTER (WHERE typedroit LIKE 'P') as a_prop_cod_prop,
			ARRAY_AGG(prop2.idprop) FILTER (WHERE typedroit LIKE 'P') as a_prop_id,
			ARRAY_AGG(prop2.idu) FILTER (WHERE typedroit LIKE 'P') as a_prop_idu,
			ARRAY_AGG(prop2.public) FILTER (WHERE typedroit LIKE 'P') as a_prop_public,
			ARRAY_AGG(prop2.cominterco) FILTER (WHERE typedroit LIKE 'P') as a_prop_cominterco,
			ARRAY_AGG(prop2.com_niv) FILTER (WHERE typedroit LIKE 'P') as a_prop_com_niv,
			ARRAY_AGG(prop2.com_niv_libelle) FILTER (WHERE typedroit LIKE 'P') as a_prop_com_niv_libelle,
			ARRAY_AGG(prop2.cod_prop) FILTER (WHERE typedroit LIKE 'G') as a_ges_cod_prop,
			ARRAY_AGG(prop2.idprop) FILTER (WHERE typedroit LIKE 'G') as a_ges_id,
			ARRAY_AGG(prop2.idu) FILTER (WHERE typedroit LIKE 'G') as a_ges_idu,
			ARRAY_AGG(prop2.public) FILTER (WHERE typedroit LIKE 'G') as a_ges_public,
			ARRAY_AGG(prop2.cominterco) FILTER (WHERE typedroit LIKE 'G') as a_ges_cominterco,
			ARRAY_AGG(prop2.com_niv) FILTER (WHERE typedroit LIKE 'G') as a_ges_com_niv,
			ARRAY_AGG(prop2.com_niv_libelle) FILTER (WHERE typedroit LIKE 'G') as a_ges_com_niv_libelle,
			Null AS idcom22
		FROM public_par par
		JOIN public_prop2 prop2 ON par.idpar = prop2.idpar
		GROUP BY par.idpar, idsec, idprocpte, idparref, idsecref, idvoie, idcom, idcomtxt, ccodep, ccodir, ccocom, ccopre, ccosec, dnupla,
			dcntpa, dsrpar, dnupro, jdatat, jdatatv, dreflf, gpdl, cprsecr, ccosecr, dnuplar, dnupdl, gurbpa, dparpi, ccoarp, gparnf,
			gparbat, dnvoiri, dindic, ccovoi, ccoriv, ccocif, cconvo, dvoilib, idparm, ccocomm, ccoprem, ccosecm, dnuplam, type, typetxt,
			jdatatan, jannatmin, jannatmax, jannatminh, jannatmaxh, janbilmin, nsuf, ssuf, cgrnumd, cgrnumdtxt, dcntsfd, dcntsol, dcntagri,
			dcntbois, dcntnat, dcnteau, dcnt01, dcnt02, dcnt03, dcnt04, dcnt06, dcnt07, dcnt09, dcnt10, dcnt11, dcnt12, dcnt13, schemrem,
			nlocal, nlocmaison, nlocappt, nlochabit, nloccom, nloccomrdc, nloccomter, nloccomsec, nlocdep, nlocburx, tlocdomin, nbat,
			npevaffh, npevph, stoth, smoyh, npiecemoy, stotdsueic, nhabvacant, nactvacant, nmediocre, nloghlm, noccprop, nocclocat, typoocc,
			npevp, stotp, smoyp, npevd, stotd, smoyd, spevtot, tpevdom_n, tpevdom_s, nlot, pdlmp, ctpdl, ndroit, ndroitindi, ndroitpro,
			ndroitges, typprop, typproptxt, typproppro, typpropprotxt, typpropges, typpropgestxt, locprop, locproptxt, geomloc, geompar,
			source_geo, vecteur, idpk;

		\$\$, 
			--public_par
			schema_name, table_name_parcelle,
			schema_name, table_name_prop,
			typedroit_var,
			--public_prop
			typedroit_var,
			schema_name, table_name_prop,
			--nat.public_XX
			table_name_new);
	
	
	ELSIF millesime LIKE '2014' THEN
		typedroit_var = 'typedroit';
		typpro = 'P';

		EXECUTE format(
		\$\$
		--Extraction idpar à au moins 1 propriétaire public (5m)
		WITH public_par AS (
			SELECT DISTINCT par.*
			FROM %I.%I par
			JOIN %I.%I prop1 ON par.ccodep = prop1.ccodep
				AND par.idprocpte = prop1.idprocpte
			JOIN prop_mm.proprio_final_m2021_work prop2 ON prop1.idprodroit = prop2.idprodroit
			WHERE prop1.%I LIKE %L AND prop2.public IS TRUE),

		--Extraction de tous les propriétaires et gestionnaires de ces parcelles (5m30s)
		public_prop AS (
			SELECT DISTINCT par.idpar, prop1.idprodroit, prop1.typedroit
			FROM public_par par
			JOIN %I.%I prop1 ON par.ccodep = prop1.ccodep
				AND par.idprocpte = prop1.idprocpte),

		---Extraction des infos propriétaires et gestionnaires personnes morales (16s)
		public_prop2 AS (
			SELECT prop1.idpar, prop1.typedroit,
				prop2.id AS idprop, prop2.idu, COALESCE(prop2.cod_prop,'X1') cod_prop,
				COALESCE(prop2.public,'False') public, COALESCE(prop2.cominterco, 'False') cominterco,
				prop2.com_niv, prop2.com_niv_libelle
			FROM public_prop prop1
			LEFT JOIN prop_mm.proprio_final_m2021_work prop2 ON prop1.idprodroit = prop2.idprodroit)

		---Extraction des parcelles à au moins 1 propriétaire public + aggrégation des propriétaires(2m01) 
		INSERT INTO nat.%I
			SELECT par.idpar, idsec, idprocpte, idparref, idsecref, idvoie, idcom, idcomtxt, ccodep, ccodir, ccocom, ccopre, ccosec,
			dnupla, dcntpa, dsrpar, dnupro, jdatat, jdatatv, dreflf, gpdl, cprsecr, ccosecr, dnuplar, dnupdl, gurbpa, dparpi, ccoarp,
			gparnf, gparbat, dnuvoi, dindic, ccovoi, ccoriv, ccocif, cconvo, dvoilib, idparm, ccocomm, ccoprem, ccosecm, dnuplam, type,
			typetxt, jdatatan, nmutpar5a, jannatmin, jannatmax, jannatminh, jannatmaxh, janbilmin, nsuf, ssuf, cgrnumd, cgrnumdtxt,
			dcntsfd, dcntsol, dcntagri, dcntbois, dcntnat, dcnteau, dcnt01, dcnt02, dcnt03, dcnt04, dcnt06, dcnt07, dcnt09, dcnt10,
			dcnt11, dcnt12, dcnt13, schemrem, nlocal, nlocmaison, nlocappt, nloclog, nloccom, nloccomrdc, nloccomter, ncomtersd, ncomterdep,
			nloccomsec, nlocdep, nlocburx, tlocdomin, nbat, nlochab, nlogh, npevph, stoth, stotdsueic, nloghvac, nloghmeu, nloghloue,
			nloghpp, nloghautre, nloghnonh, nhabvacant, nactvacant, nloghvac2a, nactvac2a, nloghvac5a, nactvac5a, nmediocre, nloghlm, npevp,
			stotp, npevd, stotd, spevtot, tpevdom_s, nlot, pdlmp, ctpdl, typecopro, ncp, ndroit, ndroitindi, ndroitpro, ndroitges, typprop,
			typproptxt, typproppro, typpropprotxt, typpropges, typpropgestxt, locprop, locproptxt, geomloc, geompar, source_geo, vecteur,
			contour, idpk,
			ARRAY_AGG(prop2.cod_prop) FILTER (WHERE typedroit LIKE 'P') as a_prop_cod_prop,
			ARRAY_AGG(prop2.idprop) FILTER (WHERE typedroit LIKE 'P') as a_prop_id,
			ARRAY_AGG(prop2.idu) FILTER (WHERE typedroit LIKE 'P') as a_prop_idu,
			ARRAY_AGG(prop2.public) FILTER (WHERE typedroit LIKE 'P') as a_prop_public,
			ARRAY_AGG(prop2.cominterco) FILTER (WHERE typedroit LIKE 'P') as a_prop_cominterco,
			ARRAY_AGG(prop2.com_niv) FILTER (WHERE typedroit LIKE 'P') as a_prop_com_niv,
			ARRAY_AGG(prop2.com_niv_libelle) FILTER (WHERE typedroit LIKE 'P') as a_prop_com_niv_libelle,
			ARRAY_AGG(prop2.cod_prop) FILTER (WHERE typedroit LIKE 'G') as a_ges_cod_prop,
			ARRAY_AGG(prop2.idprop) FILTER (WHERE typedroit LIKE 'G') as a_ges_id,
			ARRAY_AGG(prop2.idu) FILTER (WHERE typedroit LIKE 'G') as a_ges_idu,
			ARRAY_AGG(prop2.public) FILTER (WHERE typedroit LIKE 'G') as a_ges_public,
			ARRAY_AGG(prop2.cominterco) FILTER (WHERE typedroit LIKE 'G') as a_ges_cominterco,
			ARRAY_AGG(prop2.com_niv) FILTER (WHERE typedroit LIKE 'G') as a_ges_com_niv,
			ARRAY_AGG(prop2.com_niv_libelle) FILTER (WHERE typedroit LIKE 'G') as a_ges_com_niv_libelle,
			Null AS idcom22
		FROM public_par par
		JOIN public_prop2 prop2 ON par.idpar = prop2.idpar
		GROUP BY par.idpar, idsec, idprocpte, idparref, idsecref, idvoie, idcom, idcomtxt, ccodep, ccodir, ccocom, ccopre, ccosec,
			dnupla, dcntpa, dsrpar, dnupro, jdatat, jdatatv, dreflf, gpdl, cprsecr, ccosecr, dnuplar, dnupdl, gurbpa, dparpi, ccoarp,
			gparnf, gparbat, dnuvoi, dindic, ccovoi, ccoriv, ccocif, cconvo, dvoilib, idparm, ccocomm, ccoprem, ccosecm, dnuplam, type,
			typetxt, jdatatan, nmutpar5a, jannatmin, jannatmax, jannatminh, jannatmaxh, janbilmin, nsuf, ssuf, cgrnumd, cgrnumdtxt,
			dcntsfd, dcntsol, dcntagri, dcntbois, dcntnat, dcnteau, dcnt01, dcnt02, dcnt03, dcnt04, dcnt06, dcnt07, dcnt09, dcnt10,
			dcnt11, dcnt12, dcnt13, schemrem, nlocal, nlocmaison, nlocappt, nloclog, nloccom, nloccomrdc, nloccomter, ncomtersd, ncomterdep,
			nloccomsec, nlocdep, nlocburx, tlocdomin, nbat, nlochab, nlogh, npevph, stoth, stotdsueic, nloghvac, nloghmeu, nloghloue,
			nloghpp, nloghautre, nloghnonh, nhabvacant, nactvacant, nloghvac2a, nactvac2a, nloghvac5a, nactvac5a, nmediocre, nloghlm, npevp,
			stotp, npevd, stotd, spevtot, tpevdom_s, nlot, pdlmp, ctpdl, typecopro, ncp, ndroit, ndroitindi, ndroitpro, ndroitges, typprop,
			typproptxt, typproppro, typpropprotxt, typpropges, typpropgestxt, locprop, locproptxt, geomloc, geompar, source_geo, vecteur,
			contour, idpk;

		\$\$, 
			--public_par
			schema_name, table_name_parcelle,
			schema_name, table_name_prop,
			typedroit_var, typpro,
			--public_prop
			schema_name, table_name_prop,
			--nat.public_XX
			table_name_new);
	
	
	ELSIF millesime LIKE '2015' THEN
		typedroit_var = 'typedroit';
		typpro = 'P';

		EXECUTE format(
		\$\$
		--Extraction idpar à au moins 1 propriétaire public (5m)
		WITH public_par AS (
			SELECT DISTINCT par.*
			FROM %I.%I par
			JOIN %I.%I prop1 ON par.ccodep = prop1.ccodep
				AND par.idprocpte = prop1.idprocpte
			JOIN prop_mm.proprio_final_m2021_work prop2 ON prop1.idprodroit = prop2.idprodroit
			WHERE prop1.%I LIKE %L AND prop2.public IS TRUE),

		--Extraction de tous les propriétaires et gestionnaires de ces parcelles (5m30s)
		public_prop AS (
			SELECT DISTINCT par.idpar, prop1.idprodroit, prop1.typedroit
			FROM public_par par
			JOIN %I.%I prop1 ON par.ccodep = prop1.ccodep
				AND par.idprocpte = prop1.idprocpte),

		---Extraction des infos propriétaires et gestionnaires personnes morales (16s)
		public_prop2 AS (
			SELECT prop1.idpar, prop1.typedroit,
				prop2.id AS idprop, prop2.idu, COALESCE(prop2.cod_prop,'X1') cod_prop,
				COALESCE(prop2.public,'False') public, COALESCE(prop2.cominterco, 'False') cominterco,
				prop2.com_niv, prop2.com_niv_libelle
			FROM public_prop prop1
			LEFT JOIN prop_mm.proprio_final_m2021_work prop2 ON prop1.idprodroit = prop2.idprodroit)

		---Extraction des parcelles à au moins 1 propriétaire public + aggrégation des propriétaires(2m01) 
		INSERT INTO nat.%I
			SELECT par.idpar, idsec, idprocpte, idparref, idsecref, idvoie, idcom, idcomtxt, ccodep, ccodir, ccocom, ccopre, ccosec, dnupla,
			dcntpa, dsrpar, dnupro, jdatat, jdatatv, dreflf, gpdl, cprsecr, ccosecr, dnuplar, dnupdl, gurbpa, dparpi, ccoarp, gparnf,
			gparbat, dnuvoi, dindic, ccovoi, ccoriv, ccocif, cconvo, dvoilib, idparm, ccocomm, ccoprem, ccosecm, dnuplam, type, typetxt,
			jdatatan, jannatmin, jannatmax, jannatminh, jannatmaxh, janbilmin, nsuf, ssuf, cgrnumd, cgrnumdtxt, dcntsfd, dcntsol, dcntagri,
			dcntbois, dcntnat, dcnteau, dcnt01, dcnt02, dcnt03, dcnt04, dcnt06, dcnt07, dcnt09, dcnt10, dcnt11, dcnt12, dcnt13, schemrem,
			nlocal, nlocmaison, nlocappt, nloclog, nloccom, nloccomrdc, nloccomter, ncomtersd, ncomterdep, nloccomsec, nlocdep, nlocburx,
			tlocdomin, nbat, nlochab, nlogh, npevph, stoth, stotdsueic, nloghvac, nloghmeu, nloghloue, nloghpp, nloghautre, nloghnonh,
			nhabvacant, nactvacant, nloghvac2a, nactvac2a, nloghvac5a, nactvac5a, nmediocre, nloghlm, npevp, stotp, npevd, stotd, spevtot,
			tpevdom_s, nlot, pdlmp, ctpdl, typecopro, ncp, ndroit, ndroitindi, ndroitpro, ndroitges, typprop, typproptxt, typproppro,
			typpropprotxt, typpropges, typpropgestxt, locprop, locproptxt, geomloc, geompar, source_geo, vecteur, contour, idpk,
			ARRAY_AGG(prop2.cod_prop) FILTER (WHERE typedroit LIKE 'P') as a_prop_cod_prop,
			ARRAY_AGG(prop2.idprop) FILTER (WHERE typedroit LIKE 'P') as a_prop_id,
			ARRAY_AGG(prop2.idu) FILTER (WHERE typedroit LIKE 'P') as a_prop_idu,
			ARRAY_AGG(prop2.public) FILTER (WHERE typedroit LIKE 'P') as a_prop_public,
			ARRAY_AGG(prop2.cominterco) FILTER (WHERE typedroit LIKE 'P') as a_prop_cominterco,
			ARRAY_AGG(prop2.com_niv) FILTER (WHERE typedroit LIKE 'P') as a_prop_com_niv,
			ARRAY_AGG(prop2.com_niv_libelle) FILTER (WHERE typedroit LIKE 'P') as a_prop_com_niv_libelle,
			ARRAY_AGG(prop2.cod_prop) FILTER (WHERE typedroit LIKE 'G') as a_ges_cod_prop,
			ARRAY_AGG(prop2.idprop) FILTER (WHERE typedroit LIKE 'G') as a_ges_id,
			ARRAY_AGG(prop2.idu) FILTER (WHERE typedroit LIKE 'G') as a_ges_idu,
			ARRAY_AGG(prop2.public) FILTER (WHERE typedroit LIKE 'G') as a_ges_public,
			ARRAY_AGG(prop2.cominterco) FILTER (WHERE typedroit LIKE 'G') as a_ges_cominterco,
			ARRAY_AGG(prop2.com_niv) FILTER (WHERE typedroit LIKE 'G') as a_ges_com_niv,
			ARRAY_AGG(prop2.com_niv_libelle) FILTER (WHERE typedroit LIKE 'G') as a_ges_com_niv_libelle,
			Null AS idcom22
		FROM public_par par
		JOIN public_prop2 prop2 ON par.idpar = prop2.idpar
		GROUP BY par.idpar, idsec, idprocpte, idparref, idsecref, idvoie, idcom, idcomtxt, ccodep, ccodir, ccocom, ccopre, ccosec, dnupla,
			dcntpa, dsrpar, dnupro, jdatat, jdatatv, dreflf, gpdl, cprsecr, ccosecr, dnuplar, dnupdl, gurbpa, dparpi, ccoarp, gparnf,
			gparbat, dnuvoi, dindic, ccovoi, ccoriv, ccocif, cconvo, dvoilib, idparm, ccocomm, ccoprem, ccosecm, dnuplam, type, typetxt,
			jdatatan, jannatmin, jannatmax, jannatminh, jannatmaxh, janbilmin, nsuf, ssuf, cgrnumd, cgrnumdtxt, dcntsfd, dcntsol, dcntagri,
			dcntbois, dcntnat, dcnteau, dcnt01, dcnt02, dcnt03, dcnt04, dcnt06, dcnt07, dcnt09, dcnt10, dcnt11, dcnt12, dcnt13, schemrem,
			nlocal, nlocmaison, nlocappt, nloclog, nloccom, nloccomrdc, nloccomter, ncomtersd, ncomterdep, nloccomsec, nlocdep, nlocburx,
			tlocdomin, nbat, nlochab, nlogh, npevph, stoth, stotdsueic, nloghvac, nloghmeu, nloghloue, nloghpp, nloghautre, nloghnonh,
			nhabvacant, nactvacant, nloghvac2a, nactvac2a, nloghvac5a, nactvac5a, nmediocre, nloghlm, npevp, stotp, npevd, stotd, spevtot,
			tpevdom_s, nlot, pdlmp, ctpdl, typecopro, ncp, ndroit, ndroitindi, ndroitpro, ndroitges, typprop, typproptxt, typproppro,
			typpropprotxt, typpropges, typpropgestxt, locprop, locproptxt, geomloc, geompar, source_geo, vecteur, contour, idpk;
			
		\$\$, 
			--public_par
			schema_name, table_name_parcelle,
			schema_name, table_name_prop,
			typedroit_var, typpro,
			--public_prop
			schema_name, table_name_prop,
			--nat.public_XX
			table_name_new);
	
	
	ELSIF millesime LIKE '2016' THEN
		typedroit_var = 'typedroit';
		typpro = 'P';

		EXECUTE format(
		\$\$
		--Extraction idpar à au moins 1 propriétaire public (5m)
		WITH public_par AS (
			SELECT DISTINCT par.*
			FROM %I.%I par
			JOIN %I.%I prop1 ON par.ccodep = prop1.ccodep
				AND par.idprocpte = prop1.idprocpte
			JOIN prop_mm.proprio_final_m2021_work prop2 ON prop1.idprodroit = prop2.idprodroit
			WHERE prop1.%I LIKE %L AND prop2.public IS TRUE),

		--Extraction de tous les propriétaires et gestionnaires de ces parcelles (5m30s)
		public_prop AS (
			SELECT DISTINCT par.idpar, prop1.idprodroit, prop1.typedroit
			FROM public_par par
			JOIN %I.%I prop1 ON par.ccodep = prop1.ccodep
				AND par.idprocpte = prop1.idprocpte),

		---Extraction des infos propriétaires et gestionnaires personnes morales (16s)
		public_prop2 AS (
			SELECT prop1.idpar, prop1.typedroit,
				prop2.id AS idprop, prop2.idu, COALESCE(prop2.cod_prop,'X1') cod_prop,
				COALESCE(prop2.public,'False') public, COALESCE(prop2.cominterco, 'False') cominterco,
				prop2.com_niv, prop2.com_niv_libelle
			FROM public_prop prop1
			LEFT JOIN prop_mm.proprio_final_m2021_work prop2 ON prop1.idprodroit = prop2.idprodroit)

		---Extraction des parcelles à au moins 1 propriétaire public + aggrégation des propriétaires(2m01) 
		INSERT INTO nat.%I
			SELECT par.idpar, idsec, idprocpte, idparref, idsecref, idvoie, idcom, idcomtxt, ccodep, ccodir, ccocom, ccopre, ccosec, dnupla,
			dcntpa, dsrpar, dnupro, jdatat, jdatatv, dreflf, gpdl, cprsecr, ccosecr, dnuplar, dnupdl, gurbpa, dparpi, ccoarp, gparnf,
			gparbat, dnuvoi, dindic, ccovoi, ccoriv, ccocif, cconvo, dvoilib, idparm, ccocomm, ccoprem, ccosecm, dnuplam, type, typetxt,
			jdatatan, jannatmin, jannatmax, jannatminh, jannatmaxh, janbilmin, nsuf, ssuf, cgrnumd, cgrnumdtxt, dcntsfd, dcntsol, dcntarti,
			dcntagri, dcntbois, dcntnat, dcnteau, dcnt01, dcnt02, dcnt03, dcnt04, dcnt06, dcnt07, dcnt09, dcnt10, dcnt11, dcnt12, dcnt13,
			schemrem, nlocal, nlocmaison, nlocappt, nloclog, nloccom, nloccomrdc, nloccomter, ncomtersd, ncomterdep, nloccomsec, nlocdep,
			nlocburx, tlocdomin, nbat, nlochab, nlogh, npevph, stoth, stotdsueic, nloghvac, nloghmeu, nloghloue, nloghpp, nloghautre,
			nloghnonh, nhabvacant, nactvacant, nloghvac2a, nactvac2a, nloghvac5a, nactvac5a, nmediocre, nloghlm, nloghlls, npevp, stotp,
			npevd, stotd, spevtot, tpevdom_s, nlot, pdlmp, ctpdl, typecopro, ncp, ndroit, ndroitindi, ndroitpro, ndroitges, typprop,
			typproptxt, typproppro, typpropprotxt, typpropges, typpropgestxt, locprop, locproptxt, geomloc, geompar, source_geo, vecteur,
			contour, idpk,
			ARRAY_AGG(prop2.cod_prop) FILTER (WHERE typedroit LIKE 'P') as a_prop_cod_prop,
			ARRAY_AGG(prop2.idprop) FILTER (WHERE typedroit LIKE 'P') as a_prop_id,
			ARRAY_AGG(prop2.idu) FILTER (WHERE typedroit LIKE 'P') as a_prop_idu,
			ARRAY_AGG(prop2.public) FILTER (WHERE typedroit LIKE 'P') as a_prop_public,
			ARRAY_AGG(prop2.cominterco) FILTER (WHERE typedroit LIKE 'P') as a_prop_cominterco,
			ARRAY_AGG(prop2.com_niv) FILTER (WHERE typedroit LIKE 'P') as a_prop_com_niv,
			ARRAY_AGG(prop2.com_niv_libelle) FILTER (WHERE typedroit LIKE 'P') as a_prop_com_niv_libelle,
			ARRAY_AGG(prop2.cod_prop) FILTER (WHERE typedroit LIKE 'G') as a_ges_cod_prop,
			ARRAY_AGG(prop2.idprop) FILTER (WHERE typedroit LIKE 'G') as a_ges_id,
			ARRAY_AGG(prop2.idu) FILTER (WHERE typedroit LIKE 'G') as a_ges_idu,
			ARRAY_AGG(prop2.public) FILTER (WHERE typedroit LIKE 'G') as a_ges_public,
			ARRAY_AGG(prop2.cominterco) FILTER (WHERE typedroit LIKE 'G') as a_ges_cominterco,
			ARRAY_AGG(prop2.com_niv) FILTER (WHERE typedroit LIKE 'G') as a_ges_com_niv,
			ARRAY_AGG(prop2.com_niv_libelle) FILTER (WHERE typedroit LIKE 'G') as a_ges_com_niv_libelle,
			Null AS idcom22
		FROM public_par par
		JOIN public_prop2 prop2 ON par.idpar = prop2.idpar
		GROUP BY par.idpar, idsec, idprocpte, idparref, idsecref, idvoie, idcom, idcomtxt, ccodep, ccodir, ccocom, ccopre, ccosec, dnupla,
			dcntpa, dsrpar, dnupro, jdatat, jdatatv, dreflf, gpdl, cprsecr, ccosecr, dnuplar, dnupdl, gurbpa, dparpi, ccoarp, gparnf,
			gparbat, dnuvoi, dindic, ccovoi, ccoriv, ccocif, cconvo, dvoilib, idparm, ccocomm, ccoprem, ccosecm, dnuplam, type, typetxt,
			jdatatan, jannatmin, jannatmax, jannatminh, jannatmaxh, janbilmin, nsuf, ssuf, cgrnumd, cgrnumdtxt, dcntsfd, dcntsol, dcntarti,
			dcntagri, dcntbois, dcntnat, dcnteau, dcnt01, dcnt02, dcnt03, dcnt04, dcnt06, dcnt07, dcnt09, dcnt10, dcnt11, dcnt12, dcnt13,
			schemrem, nlocal, nlocmaison, nlocappt, nloclog, nloccom, nloccomrdc, nloccomter, ncomtersd, ncomterdep, nloccomsec, nlocdep,
			nlocburx, tlocdomin, nbat, nlochab, nlogh, npevph, stoth, stotdsueic, nloghvac, nloghmeu, nloghloue, nloghpp, nloghautre,
			nloghnonh, nhabvacant, nactvacant, nloghvac2a, nactvac2a, nloghvac5a, nactvac5a, nmediocre, nloghlm, nloghlls, npevp, stotp,
			npevd, stotd, spevtot, tpevdom_s, nlot, pdlmp, ctpdl, typecopro, ncp, ndroit, ndroitindi, ndroitpro, ndroitges, typprop,
			typproptxt, typproppro, typpropprotxt, typpropges, typpropgestxt, locprop, locproptxt, geomloc, geompar, source_geo, vecteur,
			contour, idpk;

		\$\$, 
			--public_par
			schema_name, table_name_parcelle,
			schema_name, table_name_prop,
			typedroit_var, typpro,
			--public_prop
			schema_name, table_name_prop,
			--nat.public_XX
			table_name_new);

	
	ELSIF millesime LIKE '2017' THEN
		typedroit_var = 'typedroit';
		typpro = 'P';

		EXECUTE format(
		\$\$
		--Extraction idpar à au moins 1 propriétaire public (5m)
		WITH public_par AS (
			SELECT DISTINCT par.*
			FROM %I.%I par
			JOIN %I.%I prop1 ON par.ccodep = prop1.ccodep
				AND par.idprocpte = prop1.idprocpte
			JOIN prop_mm.proprio_final_m2021_work prop2 ON prop1.idprodroit = prop2.idprodroit
			WHERE prop1.%I LIKE %L AND prop2.public IS TRUE),

		--Extraction de tous les propriétaires et gestionnaires de ces parcelles (5m30s)
		public_prop AS (
			SELECT DISTINCT par.idpar, prop1.idprodroit, prop1.typedroit
			FROM public_par par
			JOIN %I.%I prop1 ON par.ccodep = prop1.ccodep
				AND par.idprocpte = prop1.idprocpte),

		---Extraction des infos propriétaires et gestionnaires personnes morales (16s)
		public_prop2 AS (
			SELECT prop1.idpar, prop1.typedroit,
				prop2.id AS idprop, prop2.idu, COALESCE(prop2.cod_prop,'X1') cod_prop,
				COALESCE(prop2.public,'False') public, COALESCE(prop2.cominterco, 'False') cominterco,
				prop2.com_niv, prop2.com_niv_libelle
			FROM public_prop prop1
			LEFT JOIN prop_mm.proprio_final_m2021_work prop2 ON prop1.idprodroit = prop2.idprodroit)

		---Extraction des parcelles à au moins 1 propriétaire public + aggrégation des propriétaires(2m01) 
		INSERT INTO nat.%I
			SELECT par.idpar, idsec, idprocpte, idparref, idsecref, idvoie, idcom, idcomtxt, ccodep, ccodir, ccocom, ccopre, ccosec, dnupla,
			dcntpa, dsrpar, dnupro, jdatat, jdatatv, dreflf, gpdl, cprsecr, ccosecr, dnuplar, dnupdl, gurbpa, dparpi, ccoarp, gparnf,
			gparbat, dnuvoi, dindic, ccovoi, ccoriv, ccocif, cconvo, dvoilib, idparm, ccocomm, ccoprem, ccosecm, dnuplam, type, typetxt,
			ccoifp, jdatatan, jannatmin, jannatmax, jannatminh, jannatmaxh, janbilmin, nsuf, ssuf, cgrnumd, cgrnumdtxt, dcntsfd, dcntarti,
			dcntnaf, dcnt01, dcnt02, dcnt03, dcnt04, dcnt05, dcnt06, dcnt07, dcnt08, dcnt09, dcnt10, dcnt11, dcnt12, dcnt13, schemrem,
			nlocal, nlocmaison, nlocappt, nloclog, nloccom, nloccomrdc, nloccomter, ncomtersd, ncomterdep, nloccomsec, nlocdep, nlocburx,
			tlocdomin, nbat, nlochab, nlogh, npevph, stoth, stotdsueic, nloghvac, nloghmeu, nloghloue, nloghpp, nloghautre, nloghnonh,
			nhabvacant, nactvacant, nloghvac2a, nactvac2a, nloghvac5a, nactvac5a, nmediocre, nloghlm, nloghlls, npevp, sprobati, sprotot,
			npevd, stotd, spevtot, tpevdom_s, nlot, pdlmp, ctpdl, typecopro2, ncp, ndroit, ndroitindi, ndroitpro, ndroitges, typprop,
			typproptxt, typproppro, typpropprotxt, typpropges, typpropgestxt, locprop, locproptxt, geomloc, geompar, source_geo, vecteur,
			contour, idpk,
			ARRAY_AGG(prop2.cod_prop) FILTER (WHERE typedroit LIKE 'P') as a_prop_cod_prop,
			ARRAY_AGG(prop2.idprop) FILTER (WHERE typedroit LIKE 'P') as a_prop_id,
			ARRAY_AGG(prop2.idu) FILTER (WHERE typedroit LIKE 'P') as a_prop_idu,
			ARRAY_AGG(prop2.public) FILTER (WHERE typedroit LIKE 'P') as a_prop_public,
			ARRAY_AGG(prop2.cominterco) FILTER (WHERE typedroit LIKE 'P') as a_prop_cominterco,
			ARRAY_AGG(prop2.com_niv) FILTER (WHERE typedroit LIKE 'P') as a_prop_com_niv,
			ARRAY_AGG(prop2.com_niv_libelle) FILTER (WHERE typedroit LIKE 'P') as a_prop_com_niv_libelle,
			ARRAY_AGG(prop2.cod_prop) FILTER (WHERE typedroit LIKE 'G') as a_ges_cod_prop,
			ARRAY_AGG(prop2.idprop) FILTER (WHERE typedroit LIKE 'G') as a_ges_id,
			ARRAY_AGG(prop2.idu) FILTER (WHERE typedroit LIKE 'G') as a_ges_idu,
			ARRAY_AGG(prop2.public) FILTER (WHERE typedroit LIKE 'G') as a_ges_public,
			ARRAY_AGG(prop2.cominterco) FILTER (WHERE typedroit LIKE 'G') as a_ges_cominterco,
			ARRAY_AGG(prop2.com_niv) FILTER (WHERE typedroit LIKE 'G') as a_ges_com_niv,
			ARRAY_AGG(prop2.com_niv_libelle) FILTER (WHERE typedroit LIKE 'G') as a_ges_com_niv_libelle,
			Null AS idcom22
		FROM public_par par
		JOIN public_prop2 prop2 ON par.idpar = prop2.idpar
		GROUP BY par.idpar, idsec, idprocpte, idparref, idsecref, idvoie, idcom, idcomtxt, ccodep, ccodir, ccocom, ccopre, ccosec, dnupla,
			dcntpa, dsrpar, dnupro, jdatat, jdatatv, dreflf, gpdl, cprsecr, ccosecr, dnuplar, dnupdl, gurbpa, dparpi, ccoarp, gparnf,
			gparbat, dnuvoi, dindic, ccovoi, ccoriv, ccocif, cconvo, dvoilib, idparm, ccocomm, ccoprem, ccosecm, dnuplam, type, typetxt,
			ccoifp, jdatatan, jannatmin, jannatmax, jannatminh, jannatmaxh, janbilmin, nsuf, ssuf, cgrnumd, cgrnumdtxt, dcntsfd, dcntarti,
			dcntnaf, dcnt01, dcnt02, dcnt03, dcnt04, dcnt05, dcnt06, dcnt07, dcnt08, dcnt09, dcnt10, dcnt11, dcnt12, dcnt13, schemrem,
			nlocal, nlocmaison, nlocappt, nloclog, nloccom, nloccomrdc, nloccomter, ncomtersd, ncomterdep, nloccomsec, nlocdep, nlocburx,
			tlocdomin, nbat, nlochab, nlogh, npevph, stoth, stotdsueic, nloghvac, nloghmeu, nloghloue, nloghpp, nloghautre, nloghnonh,
			nhabvacant, nactvacant, nloghvac2a, nactvac2a, nloghvac5a, nactvac5a, nmediocre, nloghlm, nloghlls, npevp, sprobati, sprotot,
			npevd, stotd, spevtot, tpevdom_s, nlot, pdlmp, ctpdl, typecopro2, ncp, ndroit, ndroitindi, ndroitpro, ndroitges, typprop,
			typproptxt, typproppro, typpropprotxt, typpropges, typpropgestxt, locprop, locproptxt, geomloc, geompar, source_geo, vecteur,
			contour, idpk;
			
		\$\$, 
			--public_par
			schema_name, table_name_parcelle,
			schema_name, table_name_prop,
			typedroit_var, typpro,
			--public_prop
			schema_name, table_name_prop,
			--nat.public_XX
			table_name_new);

	
	ELSIF millesime LIKE '2018' THEN
		typedroit_var = 'typedroit';
		typpro = 'P';

		EXECUTE format(
		\$\$
		--Extraction idpar à au moins 1 propriétaire public (5m)
		WITH public_par AS (
			SELECT DISTINCT par.*
			FROM %I.%I par
			JOIN %I.%I prop1 ON par.ccodep = prop1.ccodep
				AND par.idprocpte = prop1.idprocpte
			JOIN prop_mm.proprio_final_m2021_work prop2 ON prop1.idprodroit = prop2.idprodroit
			WHERE prop1.%I LIKE %L AND prop2.public IS TRUE),

		--Extraction de tous les propriétaires et gestionnaires de ces parcelles (5m30s)
		public_prop AS (
			SELECT DISTINCT par.idpar, prop1.idprodroit, prop1.typedroit
			FROM public_par par
			JOIN %I.%I prop1 ON par.ccodep = prop1.ccodep
				AND par.idprocpte = prop1.idprocpte),

		---Extraction des infos propriétaires et gestionnaires personnes morales (16s)
		public_prop2 AS (
			SELECT prop1.idpar, prop1.typedroit,
				prop2.id AS idprop, prop2.idu, COALESCE(prop2.cod_prop,'X1') cod_prop,
				COALESCE(prop2.public,'False') public, COALESCE(prop2.cominterco, 'False') cominterco,
				prop2.com_niv, prop2.com_niv_libelle
			FROM public_prop prop1
			LEFT JOIN prop_mm.proprio_final_m2021_work prop2 ON prop1.idprodroit = prop2.idprodroit)

		---Extraction des parcelles à au moins 1 propriétaire public + aggrégation des propriétaires(2m01) 
		INSERT INTO nat.%I
			SELECT par.idpar, idsec, idprocpte, idparref, idsecref, idvoie, idcom, idcomtxt, ccodep, ccodir, ccocom, ccopre, ccosec, dnupla,
			dcntpa, dnupro, jdatat, jdatatv, dreflf, gpdl, cprsecr, ccosecr, dnuplar, dnupdl, gurbpa, dparpi, ccoarp, gparnf, gparbat,
			dnuvoi, dindic, ccovoi, ccoriv, ccocif, cconvo, dvoilib, idparm, ccocomm, ccoprem, ccosecm, dnuplam, type, typetxt, ccoifp,
			jdatatan, jannatmin, jannatmax, jannatminh, jannatmaxh, janbilmin, nsuf, ssuf, cgrnumd, cgrnumdtxt, dcntsfd, dcntarti, dcntnaf,
			dcnt01, dcnt02, dcnt03, dcnt04, dcnt05, dcnt06, dcnt07, dcnt08, dcnt09, dcnt10, dcnt11, dcnt12, dcnt13, schemrem, nlocal,
			nlocmaison, nlocappt, nloclog, nloccom, nloccomrdc, nloccomter, ncomtersd, ncomterdep, nloccomsec, nlocdep, nlocburx, tlocdomin,
			nbat, nlochab, nlogh, npevph, stoth, stotdsueic, nloghvac, nloghmeu, nloghloue, nloghpp, nloghautre, nloghnonh, nactvacant,
			nloghvac2a, nactvac2a, nloghvac5a, nactvac5a, nmediocre, nloghlm, nloghlls, npevd, stotd, npevp, sprincp, ssecp, ssecncp,
			sparkp, sparkncp, slocal, tpevdom_s, nlot, pdlmp, ctpdl, typecopro2, ncp, ndroit, ndroitindi, ndroitpro, ndroitges, catpro2,
			catpro2txt, catpro3, catpropro2, catproges2, locprop, locproptxt, geompar, geomloc, source_geo, vecteur, contour, idpk,
			ARRAY_AGG(prop2.cod_prop) FILTER (WHERE typedroit LIKE 'P') as a_prop_cod_prop,
			ARRAY_AGG(prop2.idprop) FILTER (WHERE typedroit LIKE 'P') as a_prop_id,
			ARRAY_AGG(prop2.idu) FILTER (WHERE typedroit LIKE 'P') as a_prop_idu,
			ARRAY_AGG(prop2.public) FILTER (WHERE typedroit LIKE 'P') as a_prop_public,
			ARRAY_AGG(prop2.cominterco) FILTER (WHERE typedroit LIKE 'P') as a_prop_cominterco,
			ARRAY_AGG(prop2.com_niv) FILTER (WHERE typedroit LIKE 'P') as a_prop_com_niv,
			ARRAY_AGG(prop2.com_niv_libelle) FILTER (WHERE typedroit LIKE 'P') as a_prop_com_niv_libelle,
			ARRAY_AGG(prop2.cod_prop) FILTER (WHERE typedroit LIKE 'G') as a_ges_cod_prop,
			ARRAY_AGG(prop2.idprop) FILTER (WHERE typedroit LIKE 'G') as a_ges_id,
			ARRAY_AGG(prop2.idu) FILTER (WHERE typedroit LIKE 'G') as a_ges_idu,
			ARRAY_AGG(prop2.public) FILTER (WHERE typedroit LIKE 'G') as a_ges_public,
			ARRAY_AGG(prop2.cominterco) FILTER (WHERE typedroit LIKE 'G') as a_ges_cominterco,
			ARRAY_AGG(prop2.com_niv) FILTER (WHERE typedroit LIKE 'G') as a_ges_com_niv,
			ARRAY_AGG(prop2.com_niv_libelle) FILTER (WHERE typedroit LIKE 'G') as a_ges_com_niv_libelle,
			Null AS idcom22
		FROM public_par par
		JOIN public_prop2 prop2 ON par.idpar = prop2.idpar
		GROUP BY par.idpar, idsec, idprocpte, idparref, idsecref, idvoie, idcom, idcomtxt, ccodep, ccodir, ccocom, ccopre, ccosec, dnupla,
			dcntpa, dnupro, jdatat, jdatatv, dreflf, gpdl, cprsecr, ccosecr, dnuplar, dnupdl, gurbpa, dparpi, ccoarp, gparnf, gparbat,
			dnuvoi, dindic, ccovoi, ccoriv, ccocif, cconvo, dvoilib, idparm, ccocomm, ccoprem, ccosecm, dnuplam, type, typetxt, ccoifp,
			jdatatan, jannatmin, jannatmax, jannatminh, jannatmaxh, janbilmin, nsuf, ssuf, cgrnumd, cgrnumdtxt, dcntsfd, dcntarti, dcntnaf,
			dcnt01, dcnt02, dcnt03, dcnt04, dcnt05, dcnt06, dcnt07, dcnt08, dcnt09, dcnt10, dcnt11, dcnt12, dcnt13, schemrem, nlocal,
			nlocmaison, nlocappt, nloclog, nloccom, nloccomrdc, nloccomter, ncomtersd, ncomterdep, nloccomsec, nlocdep, nlocburx, tlocdomin,
			nbat, nlochab, nlogh, npevph, stoth, stotdsueic, nloghvac, nloghmeu, nloghloue, nloghpp, nloghautre, nloghnonh, nactvacant,
			nloghvac2a, nactvac2a, nloghvac5a, nactvac5a, nmediocre, nloghlm, nloghlls, npevd, stotd, npevp, sprincp, ssecp, ssecncp,
			sparkp, sparkncp, slocal, tpevdom_s, nlot, pdlmp, ctpdl, typecopro2, ncp, ndroit, ndroitindi, ndroitpro, ndroitges, catpro2,
			catpro2txt, catpro3, catpropro2, catproges2, locprop, locproptxt, geompar, geomloc, source_geo, vecteur, contour, idpk;
			
		\$\$, 
			--public_par
			schema_name, table_name_parcelle,
			schema_name, table_name_prop,
			typedroit_var, typpro,
			--public_prop
			schema_name, table_name_prop,
			--nat.public_XX
			table_name_new);
	
	
	ELSIF millesime LIKE '2019' THEN
		typedroit_var = 'typedroit';
		typpro = 'P';

		EXECUTE format(
		\$\$
		--Extraction idpar à au moins 1 propriétaire public (5m)
		WITH public_par AS (
			SELECT DISTINCT par.*
			FROM %I.%I par
			JOIN %I.%I prop1 ON par.ccodep = prop1.ccodep
				AND par.idprocpte = prop1.idprocpte
			JOIN prop_mm.proprio_final_m2021_work prop2 ON prop1.idprodroit = prop2.idprodroit
			WHERE prop1.%I LIKE %L AND prop2.public IS TRUE),

		--Extraction de tous les propriétaires et gestionnaires de ces parcelles (5m30s)
		public_prop AS (
			SELECT DISTINCT par.idpar, prop1.idprodroit, prop1.typedroit
			FROM public_par par
			JOIN %I.%I prop1 ON par.ccodep = prop1.ccodep
				AND par.idprocpte = prop1.idprocpte),

		---Extraction des infos propriétaires et gestionnaires personnes morales (16s)
		public_prop2 AS (
			SELECT prop1.idpar, prop1.typedroit,
				prop2.id AS idprop, prop2.idu, COALESCE(prop2.cod_prop,'X1') cod_prop,
				COALESCE(prop2.public,'False') public, COALESCE(prop2.cominterco, 'False') cominterco,
				prop2.com_niv, prop2.com_niv_libelle
			FROM public_prop prop1
			LEFT JOIN prop_mm.proprio_final_m2021_work prop2 ON prop1.idprodroit = prop2.idprodroit)

		---Extraction des parcelles à au moins 1 propriétaire public + aggrégation des propriétaires(2m01) 
		INSERT INTO nat.%I
			SELECT par.idpar, idsec, idprocpte, idparref, idsecref, idvoie, idcom, idcomtxt, ccodep, ccodir, ccocom, ccopre, ccosec, dnupla,
			dcntpa, dnupro, jdatat, jdatatv, dreflf, gpdl, cprsecr, ccosecr, dnuplar, dnupdl, gurbpa, dparpi, ccoarp, gparnf, gparbat,
			dnuvoi, dindic, ccovoi, ccoriv, ccocif, ccpper, cconvo, dvoilib, idparm, ccocomm, ccoprem, ccosecm, dnuplam, type, typetxt,
			ccoifp, jdatatan, jannatmin, jannatmax, jannatminh, jannatmaxh, janbilmin, nsuf, ssuf, cgrnumd, cgrnumdtxt, dcntsfd, dcntarti,
			dcntnaf, dcnt01, dcnt02, dcnt03, dcnt04, dcnt05, dcnt06, dcnt07, dcnt08, dcnt09, dcnt10, dcnt11, dcnt12, dcnt13, schemrem,
			nlocal, nlocmaison, nlocappt, nloclog, nloccom, nloccomrdc, nloccomter, ncomtersd, ncomterdep, nloccomsec, nlocdep, nlocburx,
			tlocdomin, nbat, nlochab, nlogh, npevph, stoth, stotdsueic, nloghvac, nloghmeu, nloghloue, nloghpp, nloghautre, nloghnonh,
			nactvacant, nloghvac2a, nactvac2a, nloghvac5a, nactvac5a, nmediocre, nloghlm, nloghlls, npevd, stotd, npevp, sprincp, ssecp,
			ssecncp, sparkp, sparkncp, slocal, tpevdom_s, nlot, pdlmp, ctpdl, typecopro2, ncp, ndroit, ndroitindi, ndroitpro, ndroitges,
			catpro2, catpro2txt, catpro3, catpropro2, catproges2, locprop, locproptxt, geompar, geomloc, source_geo, vecteur, contour, idpk,
			ARRAY_AGG(prop2.cod_prop) FILTER (WHERE typedroit LIKE 'P') as a_prop_cod_prop,
			ARRAY_AGG(prop2.idprop) FILTER (WHERE typedroit LIKE 'P') as a_prop_id,
			ARRAY_AGG(prop2.idu) FILTER (WHERE typedroit LIKE 'P') as a_prop_idu,
			ARRAY_AGG(prop2.public) FILTER (WHERE typedroit LIKE 'P') as a_prop_public,
			ARRAY_AGG(prop2.cominterco) FILTER (WHERE typedroit LIKE 'P') as a_prop_cominterco,
			ARRAY_AGG(prop2.com_niv) FILTER (WHERE typedroit LIKE 'P') as a_prop_com_niv,
			ARRAY_AGG(prop2.com_niv_libelle) FILTER (WHERE typedroit LIKE 'P') as a_prop_com_niv_libelle,
			ARRAY_AGG(prop2.cod_prop) FILTER (WHERE typedroit LIKE 'G') as a_ges_cod_prop,
			ARRAY_AGG(prop2.idprop) FILTER (WHERE typedroit LIKE 'G') as a_ges_id,
			ARRAY_AGG(prop2.idu) FILTER (WHERE typedroit LIKE 'G') as a_ges_idu,
			ARRAY_AGG(prop2.public) FILTER (WHERE typedroit LIKE 'G') as a_ges_public,
			ARRAY_AGG(prop2.cominterco) FILTER (WHERE typedroit LIKE 'G') as a_ges_cominterco,
			ARRAY_AGG(prop2.com_niv) FILTER (WHERE typedroit LIKE 'G') as a_ges_com_niv,
			ARRAY_AGG(prop2.com_niv_libelle) FILTER (WHERE typedroit LIKE 'G') as a_ges_com_niv_libelle,
			Null AS idcom22
		FROM public_par par
		JOIN public_prop2 prop2 ON par.idpar = prop2.idpar
		GROUP BY par.idpar, idsec, idprocpte, idparref, idsecref, idvoie, idcom, idcomtxt, ccodep, ccodir, ccocom, ccopre, ccosec, dnupla,
			dcntpa, dnupro, jdatat, jdatatv, dreflf, gpdl, cprsecr, ccosecr, dnuplar, dnupdl, gurbpa, dparpi, ccoarp, gparnf, gparbat,
			dnuvoi, dindic, ccovoi, ccoriv, ccocif, ccpper, cconvo, dvoilib, idparm, ccocomm, ccoprem, ccosecm, dnuplam, type, typetxt,
			ccoifp, jdatatan, jannatmin, jannatmax, jannatminh, jannatmaxh, janbilmin, nsuf, ssuf, cgrnumd, cgrnumdtxt, dcntsfd, dcntarti,
			dcntnaf, dcnt01, dcnt02, dcnt03, dcnt04, dcnt05, dcnt06, dcnt07, dcnt08, dcnt09, dcnt10, dcnt11, dcnt12, dcnt13, schemrem,
			nlocal, nlocmaison, nlocappt, nloclog, nloccom, nloccomrdc, nloccomter, ncomtersd, ncomterdep, nloccomsec, nlocdep, nlocburx,
			tlocdomin, nbat, nlochab, nlogh, npevph, stoth, stotdsueic, nloghvac, nloghmeu, nloghloue, nloghpp, nloghautre, nloghnonh,
			nactvacant, nloghvac2a, nactvac2a, nloghvac5a, nactvac5a, nmediocre, nloghlm, nloghlls, npevd, stotd, npevp, sprincp, ssecp,
			ssecncp, sparkp, sparkncp, slocal, tpevdom_s, nlot, pdlmp, ctpdl, typecopro2, ncp, ndroit, ndroitindi, ndroitpro, ndroitges,
			catpro2, catpro2txt, catpro3, catpropro2, catproges2, locprop, locproptxt, geompar, geomloc, source_geo, vecteur, contour, idpk;
			
		\$\$, 
			--public_par
			schema_name, table_name_parcelle,
			schema_name, table_name_prop,
			typedroit_var, typpro,
			--public_prop
			schema_name, table_name_prop,
			--nat.public_XX
			table_name_new);

	
	ELSIF millesime LIKE '2020' THEN
		typedroit_var = 'typedroit';
		typpro = 'P';

		EXECUTE format(
		\$\$
		--Extraction idpar à au moins 1 propriétaire public (5m)
		WITH public_par AS (
			SELECT DISTINCT par.*
			FROM %I.%I par
			JOIN %I.%I prop1 ON par.ccodep = prop1.ccodep
				AND par.idprocpte = prop1.idprocpte
			JOIN prop_mm.proprio_final_m2021_work prop2 ON prop1.idprodroit = prop2.idprodroit
			WHERE prop1.%I LIKE %L AND prop2.public IS TRUE),

		--Extraction de tous les propriétaires et gestionnaires de ces parcelles (5m30s)
		public_prop AS (
			SELECT DISTINCT par.idpar, prop1.idprodroit, prop1.typedroit
			FROM public_par par
			JOIN %I.%I prop1 ON par.ccodep = prop1.ccodep
				AND par.idprocpte = prop1.idprocpte),

		---Extraction des infos propriétaires et gestionnaires personnes morales (16s)
		public_prop2 AS (
			SELECT prop1.idpar, prop1.typedroit,
				prop2.id AS idprop, prop2.idu, COALESCE(prop2.cod_prop,'X1') cod_prop,
				COALESCE(prop2.public,'False') public, COALESCE(prop2.cominterco, 'False') cominterco,
				prop2.com_niv, prop2.com_niv_libelle
			FROM public_prop prop1
			LEFT JOIN prop_mm.proprio_final_m2021_work prop2 ON prop1.idprodroit = prop2.idprodroit)

		---Extraction des parcelles à au moins 1 propriétaire public + aggrégation des propriétaires(2m01) 
		INSERT INTO nat.%I
			SELECT par.idpar, idsec, idprocpte, idparref, idsecref, idvoie, idcom, idcomtxt, ccodep, ccodir, ccocom, ccopre, ccosec, dnupla,
			dcntpa, dnupro, jdatat, jdatatv, dreflf, gpdl, cprsecr, ccosecr, dnuplar, dnupdl, gurbpa, dparpi, ccoarp, gparnf, gparbat,
			dnuvoi, dindic, ccovoi, ccoriv, ccocif, ccpper, cconvo, dvoilib, idparm, ccocomm, ccoprem, ccosecm, dnuplam, type, typetxt,
			ccoifp, jdatatan, jannatmin, jannatmax, jannatminh, jannatmaxh, janbilmin, nsuf, ssuf, cgrnumd, cgrnumdtxt, dcntsfd, dcntarti,
			dcntnaf, dcnt01, dcnt02, dcnt03, dcnt04, dcnt05, dcnt06, dcnt07, dcnt08, dcnt09, dcnt10, dcnt11, dcnt12, dcnt13, schemrem,
			nlocal, nlocmaison, nlocappt, nloclog, nloccom, nloccomrdc, nloccomter, ncomtersd, ncomterdep, nloccomsec, nlocdep, nlocburx,
			tlocdomin, nbat, nlochab, nlogh, npevph, stoth, stotdsueic, nloghvac, nloghmeu, nloghloue, nloghpp, nloghautre, nloghnonh,
			nactvacant, nloghvac2a, nactvac2a, nloghvac5a, nactvac5a, nmediocre, nloghlm, nloghlls, npevd, stotd, npevp, sprincp, ssecp,
			ssecncp, sparkp, sparkncp, slocal, tpevdom_s, nlot, pdlmp, ctpdl, typecopro2, ncp, ndroit, ndroitindi, ndroitpro, ndroitges,
			catpro2, catpro2txt, catpro3, catpropro2, catproges2, locprop, locproptxt, geompar, geomloc, source_geo, vecteur, contour, idpk,
			ARRAY_AGG(prop2.cod_prop) FILTER (WHERE typedroit LIKE 'P') as a_prop_cod_prop,
			ARRAY_AGG(prop2.idprop) FILTER (WHERE typedroit LIKE 'P') as a_prop_id,
			ARRAY_AGG(prop2.idu) FILTER (WHERE typedroit LIKE 'P') as a_prop_idu,
			ARRAY_AGG(prop2.public) FILTER (WHERE typedroit LIKE 'P') as a_prop_public,
			ARRAY_AGG(prop2.cominterco) FILTER (WHERE typedroit LIKE 'P') as a_prop_cominterco,
			ARRAY_AGG(prop2.com_niv) FILTER (WHERE typedroit LIKE 'P') as a_prop_com_niv,
			ARRAY_AGG(prop2.com_niv_libelle) FILTER (WHERE typedroit LIKE 'P') as a_prop_com_niv_libelle,
			ARRAY_AGG(prop2.cod_prop) FILTER (WHERE typedroit LIKE 'G') as a_ges_cod_prop,
			ARRAY_AGG(prop2.idprop) FILTER (WHERE typedroit LIKE 'G') as a_ges_id,
			ARRAY_AGG(prop2.idu) FILTER (WHERE typedroit LIKE 'G') as a_ges_idu,
			ARRAY_AGG(prop2.public) FILTER (WHERE typedroit LIKE 'G') as a_ges_public,
			ARRAY_AGG(prop2.cominterco) FILTER (WHERE typedroit LIKE 'G') as a_ges_cominterco,
			ARRAY_AGG(prop2.com_niv) FILTER (WHERE typedroit LIKE 'G') as a_ges_com_niv,
			ARRAY_AGG(prop2.com_niv_libelle) FILTER (WHERE typedroit LIKE 'G') as a_ges_com_niv_libelle,
			Null AS idcom22
		FROM public_par par
		JOIN public_prop2 prop2 ON par.idpar = prop2.idpar
		GROUP BY par.idpar, idsec, idprocpte, idparref, idsecref, idvoie, idcom, idcomtxt, ccodep, ccodir, ccocom, ccopre, ccosec, dnupla,
			dcntpa, dnupro, jdatat, jdatatv, dreflf, gpdl, cprsecr, ccosecr, dnuplar, dnupdl, gurbpa, dparpi, ccoarp, gparnf, gparbat,
			dnuvoi, dindic, ccovoi, ccoriv, ccocif, ccpper, cconvo, dvoilib, idparm, ccocomm, ccoprem, ccosecm, dnuplam, type, typetxt,
			ccoifp, jdatatan, jannatmin, jannatmax, jannatminh, jannatmaxh, janbilmin, nsuf, ssuf, cgrnumd, cgrnumdtxt, dcntsfd, dcntarti,
			dcntnaf, dcnt01, dcnt02, dcnt03, dcnt04, dcnt05, dcnt06, dcnt07, dcnt08, dcnt09, dcnt10, dcnt11, dcnt12, dcnt13, schemrem,
			nlocal, nlocmaison, nlocappt, nloclog, nloccom, nloccomrdc, nloccomter, ncomtersd, ncomterdep, nloccomsec, nlocdep, nlocburx,
			tlocdomin, nbat, nlochab, nlogh, npevph, stoth, stotdsueic, nloghvac, nloghmeu, nloghloue, nloghpp, nloghautre, nloghnonh,
			nactvacant, nloghvac2a, nactvac2a, nloghvac5a, nactvac5a, nmediocre, nloghlm, nloghlls, npevd, stotd, npevp, sprincp, ssecp,
			ssecncp, sparkp, sparkncp, slocal, tpevdom_s, nlot, pdlmp, ctpdl, typecopro2, ncp, ndroit, ndroitindi, ndroitpro, ndroitges,
			catpro2, catpro2txt, catpro3, catpropro2, catproges2, locprop, locproptxt, geompar, geomloc, source_geo, vecteur, contour, idpk;

		\$\$, 
			--public_par
			schema_name, table_name_parcelle,
			schema_name, table_name_prop,
			typedroit_var, typpro,
			--public_prop
			schema_name, table_name_prop,
			--nat.public_XX
			table_name_new);
			
	
	ELSIF millesime LIKE '2021' THEN
		typedroit_var = 'typedroit';
		typpro = 'P';

		EXECUTE format(
		\$\$
		--Extraction idpar à au moins 1 propriétaire public (5m)
		WITH public_par AS (
			SELECT DISTINCT par.*
			FROM %I.%I par
			JOIN %I.%I prop1 ON par.ccodep = prop1.ccodep
				AND par.idprocpte = prop1.idprocpte
			JOIN prop_mm.proprio_final_m2021_work prop2 ON prop1.idprodroit = prop2.idprodroit
			WHERE prop1.%I LIKE %L AND prop2.public IS TRUE),

		--Extraction de tous les propriétaires et gestionnaires de ces parcelles (5m30s)
		public_prop AS (
			SELECT DISTINCT par.idpar, prop1.idprodroit, prop1.typedroit
			FROM public_par par
			JOIN %I.%I prop1 ON par.ccodep = prop1.ccodep
				AND par.idprocpte = prop1.idprocpte),

		---Extraction des infos propriétaires et gestionnaires personnes morales (16s)
		public_prop2 AS (
			SELECT prop1.idpar, prop1.typedroit,
				prop2.id AS idprop, prop2.idu, COALESCE(prop2.cod_prop,'X1') cod_prop,
				COALESCE(prop2.public,'False') public, COALESCE(prop2.cominterco, 'False') cominterco,
				prop2.com_niv, prop2.com_niv_libelle
			FROM public_prop prop1
			LEFT JOIN prop_mm.proprio_final_m2021_work prop2 ON prop1.idprodroit = prop2.idprodroit)

		---Extraction des parcelles à au moins 1 propriétaire public + aggrégation des propriétaires(2m01) 
		INSERT INTO nat.%I
			SELECT par.idpar, idtup, idsec, idprocpte, idparref, idsecref, idvoie, idcom, idcomtxt, ccodep,
			ccodir, ccocom, ccopre, ccosec, dnupla, dcntpa, dnupro, jdatat, jdatatv, dreflf, gpdl,
			cprsecr, ccosecr, dnuplar, dnupdl, gurbpa, dparpi, ccoarp, gparnf, gparbat, dnuvoi, dindic,
			ccovoi, ccoriv, ccocif, ccpper, cconvo, dvoilib, idparm, ccocomm, ccoprem, ccosecm, dnuplam,
			type, typetxt, ccoifp, jdatatan, jannatmin, jannatmax, jannatminh, jannatmaxh, janbilmin, nsuf,
			ssuf, cgrnumd, cgrnumdtxt, dcntsfd, dcntarti, dcntnaf, dcnt01, dcnt02, dcnt03, dcnt04, dcnt05,
			dcnt06, dcnt07, dcnt08, dcnt09, dcnt10, dcnt11, dcnt12, dcnt13, schemrem, nlocal, nlocmaison,
			nlocappt, nloclog, nloccom, nloccomrdc, nloccomter, ncomtersd, ncomterdep, nloccomsec, nlocdep,
			nlocburx, tlocdomin, nbat, nlochab, nlogh, nloghmais, nloghappt, npevph, stoth, stotdsueic,
			nloghvac, nloghmeu, nloghloue, nloghpp, nloghautre, nloghnonh, nactvacant, nloghvac2a,
			nactvac2a, nloghvac5a, nactvac5a, nmediocre, nloghlm, nloghlls, npevd, stotd, npevp, sprincp,
			ssecp, ssecncp, sparkp, sparkncp, slocal, tpevdom_s, nlot, pdlmp, ctpdl, typecopro2, ncp,
			ndroit, ndroitindi, ndroitpro, ndroitges, catpro2, catpro2txt, catpro3, catpropro2, catproges2,
			locprop, locproptxt, geompar, geomloc, source_geo, vecteur, contour, idpk,
			ARRAY_AGG(prop2.cod_prop) FILTER (WHERE typedroit LIKE 'P') as a_prop_cod_prop,
			ARRAY_AGG(prop2.idprop) FILTER (WHERE typedroit LIKE 'P') as a_prop_id,
			ARRAY_AGG(prop2.idu) FILTER (WHERE typedroit LIKE 'P') as a_prop_idu,
			ARRAY_AGG(prop2.public) FILTER (WHERE typedroit LIKE 'P') as a_prop_public,
			ARRAY_AGG(prop2.cominterco) FILTER (WHERE typedroit LIKE 'P') as a_prop_cominterco,
			ARRAY_AGG(prop2.com_niv) FILTER (WHERE typedroit LIKE 'P') as a_prop_com_niv,
			ARRAY_AGG(prop2.com_niv_libelle) FILTER (WHERE typedroit LIKE 'P') as a_prop_com_niv_libelle,
			ARRAY_AGG(prop2.cod_prop) FILTER (WHERE typedroit LIKE 'G') as a_ges_cod_prop,
			ARRAY_AGG(prop2.idprop) FILTER (WHERE typedroit LIKE 'G') as a_ges_id,
			ARRAY_AGG(prop2.idu) FILTER (WHERE typedroit LIKE 'G') as a_ges_idu,
			ARRAY_AGG(prop2.public) FILTER (WHERE typedroit LIKE 'G') as a_ges_public,
			ARRAY_AGG(prop2.cominterco) FILTER (WHERE typedroit LIKE 'G') as a_ges_cominterco,
			ARRAY_AGG(prop2.com_niv) FILTER (WHERE typedroit LIKE 'G') as a_ges_com_niv,
			ARRAY_AGG(prop2.com_niv_libelle) FILTER (WHERE typedroit LIKE 'G') as a_ges_com_niv_libelle,
			Null AS idcom22
		FROM public_par par
		JOIN public_prop2 prop2 ON par.idpar = prop2.idpar
		GROUP BY par.idpar, idtup, idsec, idprocpte, idparref, idsecref, idvoie, idcom, idcomtxt, ccodep,
			ccodir, ccocom, ccopre, ccosec, dnupla, dcntpa, dnupro, jdatat, jdatatv, dreflf, gpdl,
			cprsecr, ccosecr, dnuplar, dnupdl, gurbpa, dparpi, ccoarp, gparnf, gparbat, dnuvoi, dindic,
			ccovoi, ccoriv, ccocif, ccpper, cconvo, dvoilib, idparm, ccocomm, ccoprem, ccosecm, dnuplam,
			type, typetxt, ccoifp, jdatatan, jannatmin, jannatmax, jannatminh, jannatmaxh, janbilmin, nsuf,
			ssuf, cgrnumd, cgrnumdtxt, dcntsfd, dcntarti, dcntnaf, dcnt01, dcnt02, dcnt03, dcnt04, dcnt05,
			dcnt06, dcnt07, dcnt08, dcnt09, dcnt10, dcnt11, dcnt12, dcnt13, schemrem, nlocal, nlocmaison,
			nlocappt, nloclog, nloccom, nloccomrdc, nloccomter, ncomtersd, ncomterdep, nloccomsec, nlocdep,
			nlocburx, tlocdomin, nbat, nlochab, nlogh, nloghmais, nloghappt, npevph, stoth, stotdsueic,
			nloghvac, nloghmeu, nloghloue, nloghpp, nloghautre, nloghnonh, nactvacant, nloghvac2a,
			nactvac2a, nloghvac5a, nactvac5a, nmediocre, nloghlm, nloghlls, npevd, stotd, npevp, sprincp,
			ssecp, ssecncp, sparkp, sparkncp, slocal, tpevdom_s, nlot, pdlmp, ctpdl, typecopro2, ncp,
			ndroit, ndroitindi, ndroitpro, ndroitges, catpro2, catpro2txt, catpro3, catpropro2, catproges2,
			locprop, locproptxt, geompar, geomloc, source_geo, vecteur, contour, idpk;

		\$\$, 
			--public_par
			schema_name, table_name_parcelle,
			schema_name, table_name_prop,
			typedroit_var, typpro,
			--public_prop
			schema_name, table_name_prop,
			--nat.public_XX
			table_name_new);
	
	ELSE
		RAISE NOTICE 'ERREUR : Millésime %I non configuré pour l import',millesime;
		EXIT;
	
	END IF;

	COMMIT;

	RAISE NOTICE 'Recherche idcom22, iddep22 et indexation';
	
	EXECUTE format(
		\$\$
		
		---Création des index (2m49)
		CREATE INDEX nat_%I_idpar_idx ON nat.%I(idpar);
		CREATE INDEX nat_%I_geomloc_idx ON nat.%I USING gist(geomloc);
		CREATE INDEX nat_%I_geompar_idx ON nat.%I USING gist(geompar);

		--idcom22(10m)
			ALTER TABLE nat.%I DROP COLUMN IF EXISTS idcom22 CASCADE;
			ALTER TABLE nat.%I DROP COLUMN IF EXISTS idcom22 CASCADE;
			ALTER TABLE nat.%I ADD COLUMN idcom22 varchar(5);
			ALTER TABLE nat.%I DROP COLUMN IF EXISTS iddep21 CASCADE;
			ALTER TABLE nat.%I DROP COLUMN IF EXISTS iddep22 CASCADE;
			ALTER TABLE nat.%I ADD COLUMN iddep22 varchar(3);

			UPDATE nat.%I a
			SET idcom22 = b.insee_com, iddep22= b.insee_dep
			FROM insee.geocom22 b
			WHERE ST_Contains(b.geom, a.geomloc);

			UPDATE nat.%I a
			SET idcom22 =
				CASE
					WHEN idcom LIKE '132%%' THEN '13055'
					WHEN idcom LIKE '75%%' THEN '75056'
					WHEN idcom LIKE '6938%%' THEN '69123'
				END,
				iddep22 = 
				CASE
					WHEN idcom LIKE '132%%' THEN '13'
					WHEN idcom LIKE '75%%' THEN '75'
					WHEN idcom LIKE '6938%%' THEN '69'
				END
			WHERE idcom LIKE '132%%'
				OR idcom LIKE '75%%'
				OR idcom LIKE '6938%%';

			UPDATE nat.%I a
			SET idcom22 = b.codgeo_2022,
				iddep22 = c.insee_dep
			FROM insee.table_passage_idcom22 b, insee.geocom22 c
			WHERE a.idcom22 IS NULL
				AND a.idcom = b.codgeo_ini
				AND b.codgeo_2022 = c.insee_com;			

			CREATE INDEX nat_%I_idcom22_idx ON nat.%I(idcom22);
			CREATE INDEX nat_%I_iddep22_idx ON nat.%I(iddep22);
		\$\$,
		--index
		table_name_new, table_name_new,
		table_name_new, table_name_new,
		table_name_new, table_name_new,
		--idcom22
		table_name_new,
		table_name_new,
		table_name_new,
		table_name_new,
		table_name_new,
		table_name_new,
			
		table_name_new,
			
		table_name_new,
			
		table_name_new,
			
		table_name_new,table_name_new,
		table_name_new,table_name_new);
	
	COMMIT;
	RAISE NOTICE 'Import du millésime % terminé',millesime;


	END LOOP;
	RAISE NOTICE 'Import de tous les millésimes terminé';
END
\$do\$
