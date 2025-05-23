DO
$do$
DECLARE
	millesime text;
	millesime_short text;
	table_name text;
	table_communaux text;
BEGIN
	RAISE NOTICE 'Traitement multi-millésime pour affectation des propriétaires (inter)communaux démarré';
	FOREACH millesime IN ARRAY ARRAY['2021','2009','2011','2012','2013','2014','2015','2016','2017','2018','2019','2020']
	LOOP
		RAISE NOTICE 'Traitement du millésime % en cours',millesime;
		millesime_short = RIGHT(millesime,2);
		table_name = 'public'||millesime_short;
		table_communaux = 'com'||millesime_short;

		RAISE NOTICE 'identification des parcelles communales';

		EXECUTE format(
			$$
			ALTER TABLE nat.%1$s DROP COLUMN IF EXISTS cominterco CASCADE;
			ALTER TABLE nat.%1$s ADD COLUMN cominterco bool;
			ALTER TABLE nat.%1$s DROP COLUMN IF EXISTS com_niv CASCADE;
			ALTER TABLE nat.%1$s ADD COLUMN com_niv int;			
					
			--cominterco
			UPDATE nat.%1$s SET cominterco =
				CASE
					WHEN True = any(a_prop_cominterco) THEN True
					ELSE False
				END;
			CREATE INDEX nat_%1$s_cominterco_idx ON nat.%1$s(cominterco);
			$$,
			table_name
		);
		COMMIT;

		RAISE NOTICE 'attribution des niveaux de propriété';
		EXECUTE format(
			$$
			UPDATE nat.%1$s
			SET com_niv =
				CASE
					WHEN '7' = any(a_prop_com_niv)
						OR '7' = any(a_ges_com_niv)
						THEN 7---communaux
					WHEN '4' = any(a_prop_com_niv)
						OR '4' = any(a_ges_com_niv)
						THEN 4---Portage foncier
					WHEN '3' = any(a_prop_com_niv)
						OR '3' = any(a_ges_com_niv)
						THEN 3---Aménagement
					WHEN '6' = any(a_prop_com_niv)
						OR '6' = any(a_ges_com_niv)
						THEN 6---HLM
					WHEN '5' = any(a_prop_com_niv)
						OR '5' = any(a_ges_com_niv)
						OR ARRAY[FALSE] <@ a_prop_cominterco IS TRUE
						OR ARRAY[FALSE] <@ a_ges_cominterco IS TRUE
						THEN 5---Services divers
					WHEN '2' = any(a_prop_com_niv)
						OR '2' = any(a_ges_com_niv)
						THEN 2---Interco
					ELSE 1--Communes				
				END
			WHERE cominterco IS TRUE
			$$,
			table_name
		);
		COMMIT;
			
		--index
		RAISE NOTICE 'index';
		EXECUTE format(	
			$$
			CREATE INDEX nat_%1$s_cominterco_comniv_idx ON nat.%1$s(cominterco,com_niv);
			$$,
			table_name
		);
	COMMIT;
	
	RAISE NOTICE 'Traitement du millésime % terminé',millesime;
	
	END LOOP;
	RAISE NOTICE 'Traitement multi-millésime pour affectation des propriétaires (inter)communaux terminé';
END
$do$
