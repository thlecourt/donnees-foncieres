DO
$do$
DECLARE
	millesime text;
	millesime_short text;
	table_name text;
	table_communaux text;
BEGIN
	RAISE NOTICE 'Traitement multi-millésime pour affectation des propriétaires (inter)communaux démarré';
	FOREACH millesime IN ARRAY ARRAY['2009','2011';'2012','2013','2014','2015','2016','2017','2018','2019','2020','2021']
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
			
					
			--cominterco(3m32)
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

		
		---UNIQUEMENT PROPRIETAIRES COMMUNAUX
		RAISE NOTICE 'Au moins 1 non-générique';
		---Au moins 1 non-générique

		EXECUTE format(
			$$
			UPDATE nat.%1$s
			SET com_niv =
				CASE
					WHEN '7' = any(a_prop_com_niv) THEN 7---communaux
					WHEN '6' = any(a_prop_com_niv) THEN 6---HLM
					WHEN '5' = any(a_prop_com_niv) THEN 5---Services divers
					WHEN '4' = any(a_prop_com_niv) THEN 4---Portage foncier
					WHEN '3' = any(a_prop_com_niv) THEN 3---Aménagement
				END
			WHERE cominterco IS TRUE
				AND ARRAY[FALSE] <@ a_prop_cominterco IS FALSE;
			$$,
			table_name
		);
		COMMIT;

		---uniquement générique / pas de gestionnaire
		RAISE NOTICE 'pas de gestionnaire';
		EXECUTE format(
			$$
			UPDATE nat.%1$s
			SET com_niv =
				CASE
					WHEN '1' = any(a_prop_com_niv) THEN 1---Communes
					ELSE 2---Intercommunalités
				END
			WHERE cominterco IS TRUE
				AND ARRAY[FALSE] <@ a_prop_cominterco IS FALSE
				AND com_niv IS NULL
				AND a_ges_com_niv IS NULL;
			$$,
			table_name
		);
		COMMIT;

		---uniquement générique / avec gestionnaire(s)
		--HLM (9s)
		RAISE NOTICE 'avec gestionnaire';
		EXECUTE format(
			$$
			UPDATE nat.%1$s
			SET com_niv = 6
			WHERE cominterco IS TRUE
				AND ARRAY[FALSE] <@ a_prop_cominterco IS FALSE
				AND com_niv IS NULL
				AND 'F1a' = any(a_ges_cod_prop);

			--Gestionnaire communal (6s)
			UPDATE nat.%1$s
			SET com_niv =
				CASE
					WHEN '7' = any(a_ges_com_niv) THEN 7---Communaux 
					WHEN '6' = any(a_ges_com_niv) THEN 6---HLM
					WHEN '5' = any(a_ges_com_niv) THEN 5---Services
					WHEN '4' = any(a_ges_com_niv) THEN 4---Portage foncier
					WHEN '3' = any(a_ges_com_niv) THEN 3---Aménagement
					WHEN '2' = any(a_ges_com_niv) THEN 2---Interco
					ELSE 1---Commune
				END
			WHERE cominterco IS TRUE
				AND ARRAY[FALSE] <@ a_prop_cominterco IS FALSE
				AND com_niv IS NULL
				AND 'True' = any(a_ges_cominterco);

			--Gestionnaire non-communal(5s)
			UPDATE nat.%1$s
			SET com_niv = 5
			WHERE cominterco IS TRUE
				AND ARRAY[FALSE] <@ a_prop_cominterco IS FALSE
				AND com_niv IS NULL
				AND 'True' != any(a_ges_cominterco);

			--- PROPRIETAIRES COMMUNAUX ET NON-COMMUNAUX
			--Au moins 1 propriétaire HLM (5s)
			UPDATE nat.%1$s
			SET com_niv = 6
			WHERE cominterco IS TRUE
				AND ARRAY[FALSE] <@ a_prop_cominterco IS TRUE
				AND 'F1a' = any(a_prop_cod_prop);

			--Au moins 1 propriétaire privé (5s)
			UPDATE nat.%1$s
			SET com_niv = 5---Partagée avec propriété privée
			WHERE cominterco IS TRUE
				AND ARRAY[FALSE] <@ a_prop_cominterco IS TRUE
				AND com_niv IS NULL
				AND ARRAY[FALSE] <@ a_prop_public IS TRUE;

			---Sans propriétaire privé
			--pas de gestionnaire ou gestionnaire non-hlm (5s)
			UPDATE nat.%1$s
			SET com_niv = 5---Partagée avec propriété publique
			WHERE cominterco IS TRUE
				AND ARRAY[FALSE] <@ a_prop_cominterco IS TRUE
				AND com_niv IS NULL
				AND (a_ges_com_niv IS NULL
					 OR 'F1a' != any(a_ges_cod_prop));

			--gestionnaire hlm (5s)
			UPDATE nat.%1$s
			SET com_niv = 6
			WHERE cominterco IS TRUE
				AND ARRAY[FALSE] <@ a_prop_cominterco IS TRUE
				AND com_niv IS NULL
				AND 'F1a' = any(a_ges_cod_prop);
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
