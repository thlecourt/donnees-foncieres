-------------------------
---Ce script importe dans des tables dédiées (intitulées public_XX) les parcelles publiques, pour les millésimes 2009 à 2021
---Les tables vides doivent avoir été créées au préalable
-------------------------

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

	RAISE NOTICE 'Import du millésime % terminé',millesime;
	END LOOP;

RAISE NOTICE 'Import de tous les millésimes terminé';
END
\$do\$
