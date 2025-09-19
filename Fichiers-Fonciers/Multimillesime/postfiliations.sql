--- Ce script, en l'état, reconstitue les filiations postérieures des terrains obtenus par rétrofiliation dans le script retrofiliations.sql
--- m1 correspond au millésime de départ (2016), p1 au millésime suivant (2017) et p5 au millésime le plus récent (2021)

DO
\$do\$
declare
dep varchar(2);
idcom22 varchar(5);
m1 text;
partition_tablename text;
a_ccodep text[];
table_filiation_totale_dep text;
table_filiation_anterieure_dep text;

X integer;
pX text;---année à filier
pY text;---année suivante
pZ text;---année encore suivante pour les sauts de millésime
table_filiation_pX text;
table_filiation_pX_2 text;
idfil_filiation_pX text;
idfil_filiation_pX_2 text;
table_filiation_pY text;
pX_idpar text;
pX_idpar_2 text;
pY_idpar text;
pZ_idpar text;
pY_idprocpte text;
pZ_idprocpte text;
nom_contrainte_pX_pY_pZ_unique text;
nom_contrainte_pX_pY_pZ_unique_2 text;

schema_parcelles_dep_pX text;
table_parcelles_dep_pX text;
table_parcelles_dep_pX_2 text;
schema_parcelles_dep_pY text;
table_parcelles_dep_pY text;
table_parcelles_dep_pY_2 text;
schema_parcelles_dep_pZ text;
table_parcelles_dep_pZ text;
table_parcelles_dep_pZ_2 text;

millesime_de_reference text;
schema_de_reference text;
table_de_reference text;
table_de_reference2 text;

BEGIN

----------------------------------
m1 := '2016';---année d'obervation
----------------------------------

--------------------------
---Création des structures de table de filiation
--------------------------
--CREATION D'UNE TABLE TEMPORAIRE DES DEPARTEMENTS
DROP TABLE IF EXISTS anaseq.dep;
CREATE TABLE anaseq.dep AS (
	WITH sub AS (
		SELECT ccodep
		FROM anaseq.filiations_anterieures
		GROUP BY ccodep
		ORDER BY ccodep)
	SELECT ARRAY_AGG(ccodep) ccodep
	FROM sub
);
a_ccodep = (SELECT ccodep FROM anaseq.dep GROUP BY ccodep ORDER BY ccodep);

DROP TABLE IF EXISTS anaseq.filiations_totales CASCADE;
CREATE TABLE anaseq.filiations_totales (
	idpk int,
	ccodep varchar(2),
	iddep22 varchar(2),
	idcom22 varchar(5),
	annee varchar(4),
	com_niv varchar(2),
	scorpat int,
	typpat text,
	p5_idpar text,
	p4_idpar text,
	p3_idpar text,
	p2_idpar text,
	p1_idpar text,
	m1_idpar text,
	m2_idpar text,
	m3_idpar text,
	m4_idpar text,
	m5_idpar text,
	m6_idpar text,
	p5_idprocpte text,
	p4_idprocpte text,
	p3_idprocpte text,
	p2_idprocpte text,
	p1_idprocpte text,
	m1_idprocpte text,
	m2_idprocpte text,
	m3_idprocpte text,
	m4_idprocpte text,
	m5_idprocpte text,
	m6_idprocpte text,
	geom geometry,
	area numeric,
	PRIMARY KEY (idpk, ccodep)	
)
PARTITION BY LIST (ccodep);

CREATE INDEX filiations_totales_geom_idx ON anaseq.filiations_totales USING gist(geom);
CREATE INDEX filiations_totales_idcom22_idx ON anaseq.filiations_totales(idcom22);

FOREACH dep IN ARRAY ARRAY[a_ccodep]
LOOP
	RAISE NOTICE '\\\\DEPARTEMENT %////',dep;
	partition_tablename = 'filiations_totales_'||dep;
	EXECUTE format(
		\$\$ 
		CREATE TABLE IF NOT EXISTS anaseq.%I
		PARTITION OF anaseq.filiations_totales
		FOR VALUES IN (%L)
		\$\$, partition_tablename,dep);

	table_filiation_totale_dep := 'filiations_totales_'||dep;
	table_filiation_anterieure_dep := 'filiations_anterieures_'||dep;
	
	RAISE NOTICE '---FILIATIONS POSTERIEURES DES PARCELLES DE %',m1;
	
	X := 0;----Réinitialisation de la boucle
	pX := (m1::numeric);

	LOOP

		IF X > 4 THEN
			EXIT; -- Sortir de la boucle lorsque x devient supérieur à 4 (on s'arrête en 2021 si m1 = 2016)
		END IF;

		table_filiation_pY := 'filiation_p'||(X+1)::text;
		pY_idpar := 'p'||(X+1)::text||'_idpar';
		pZ_idpar := 'p'||(X+2)::text||'_idpar';
		pY_idprocpte := 'p'||(X+1)::text||'_idprocpte';
		pZ_idprocpte := 'p'||(X+2)::text||'_idprocpte';
	
		IF X = 0 THEN
			table_filiation_pX_2 := 'filiations_anterieures_'||dep;
			pX_idpar_2 := 'm1_idpar';
			idfil_filiation_pX_2 := 'idfil_m6';
			nom_contrainte_pX_pY_pZ_unique_2 := 'filiation_m6'||'_p'||(X+1)::text||'_p'||(X+2)::text||'_unique';
		ELSE
			table_filiation_pX_2 := 'filiation_p'||X::text;
			pX_idpar_2 := 'p'||(X)::text||'_idpar';
			idfil_filiation_pX_2 := 'idfil_p'||X::text;
			nom_contrainte_pX_pY_pZ_unique_2 := 'filiation_p'||(X)::text||'_p'||(X+1)::text||'_p'||(X+2)::text||'_unique';
		END IF;
		table_filiation_pX := table_filiation_pX_2;
		pX_idpar = pX_idpar_2;
		idfil_filiation_pX := idfil_filiation_pX_2;
		nom_contrainte_pX_pY_pZ_unique := nom_contrainte_pX_pY_pZ_unique_2;
		
	
		RAISE NOTICE 'Création de la structure de table p%',X+1;
	
		EXECUTE format(
			\$\$
			DROP TABLE IF EXISTS anaseq.%I CASCADE;
			CREATE TABLE anaseq.%I(
				ccodep varchar(2),
				iddep22 varchar(2),
				idcom22 varchar(5),
				annee varchar(4),---annee d'évolution initiale initiale
				%I int,---idfil_pX
				idpk SERIAL,---idfil_pY
				%I text,---pX_idpar
				%I text,---pY_idpar
				%I text,---pZ_idpar
				%I text,---pY_idprocpte
				%I text,---pZ_idprocpte
				area numeric,
				geom geometry,
				type_evol text,
				PRIMARY KEY (idpk),
				CONSTRAINT %I UNIQUE(annee,%I,%I,%I)
			)
			\$\$,
			table_filiation_pY,
			table_filiation_pY,
			idfil_filiation_pX,
			pX_idpar,
			pY_idpar,
			pZ_idpar,
			pY_idprocpte,
			pZ_idprocpte,
			nom_contrainte_pX_pY_pZ_unique, idfil_filiation_pX, pY_idpar, pZ_idpar---la contrainte portant sur idfil_filiation_pX porte en fait sur toute la chaine précédente 
			);
	
		
		pX := (pX::numeric)::text;
		pY := ((pX::numeric)+1)::text;
		pZ := ((pX::numeric)+2)::text;
		schema_parcelles_dep_pX := 'ff_'||pX||'_dep';
		schema_parcelles_dep_pY := 'ff_'||pY||'_dep';
		schema_parcelles_dep_pZ := 'ff_'||pZ||'_dep';
		pY_idpar := 'p'||(X+1)::text||'_idpar';
		pZ_idpar := 'p'||(X+2)::text||'_idpar';
		pY_idprocpte := 'p'||(X+1)::text||'_idprocpte';
		pZ_idprocpte := 'p'||(X+2)::text||'_idprocpte';
		table_filiation_pY := 'filiation_p'||(X+1)::text;

		IF pX::numeric < 2018 THEN
			table_parcelles_dep_pX_2 := 'd'||dep||'_'||pX||'_pnb10_parcelle';
		ELSE
			table_parcelles_dep_pX_2 := 'd'||dep||'_fftp_'||pX||'_pnb10_parcelle';
		END IF;
		table_parcelles_dep_pX := table_parcelles_dep_pX_2;

		IF pY::numeric < 2018 THEN
			table_parcelles_dep_pY_2 := 'd'||dep||'_'||pY||'_pnb10_parcelle';
		ELSE
			table_parcelles_dep_pY_2 := 'd'||dep||'_fftp_'||pY||'_pnb10_parcelle';
		END IF;
		table_parcelles_dep_pY := table_parcelles_dep_pY_2;
	
		IF pZ::numeric < 2018 THEN
			table_parcelles_dep_pZ_2 := 'd'||dep||'_'||pZ||'_pnb10_parcelle';
		ELSE
			table_parcelles_dep_pZ_2 := 'd'||dep||'_fftp_'||pZ||'_pnb10_parcelle';
		END IF;
		table_parcelles_dep_pZ := table_parcelles_dep_pZ_2;
	
		
		

		
		RAISE NOTICE '-----Filiations en p%-----',(X+1)::text;

		IF X > 0 THEN
			-------------------------------------------
			----SAUTS DE MILLESIME DEJA RETROUVES A L'ETAPE PRECEDENTE
			-------------------------------------------
			RAISE NOTICE 'SAUTS DE MILLESIME DEJA RETROUVES';
			EXECUTE format(
				\$\$				
				INSERT INTO anaseq.%I(---table_filiation_pY
					ccodep,
					iddep22,
					idcom22,
					annee,
					%I,---idfil_filiation_pX
					%I,---pX_idpar
					%I,---pY_idpar
					%I,---pY_idprocpte
					%I,---pZ_idpar
					area,
					geom,
					type_evol)
				
				SELECT %L,
					iddep22,
					idcom22,
					annee,
					idpk,
					%I,---pY_idpar--> 'saut'
					%I,---pZ_idpar en tant que pY
					%I,---pZ_idprocpte en tant que pY
					'SO',---sans objet
					area,
					geom,
					'saut de millésime'
				FROM anaseq.%I---table_filiation_pX
				WHERE %I LIKE 'saut'---pY_idpar
					AND annee = %L --m1
				GROUP BY iddep22,
					idcom22,
					annee,
					idpk,
					%I,
					%I,
					%I,
					area,
					geom
				\$\$,
				table_filiation_pY,
				idfil_filiation_pX,
				pX_idpar,
				pY_idpar,
				pY_idprocpte,
				pZ_idpar,
				---SELECT
				dep,
				pX_idpar, ---pY-1 car on est passé dans l'année suivante
				pY_idpar,---pZ-1 car on est passé dans l'année suivante
				pY_idprocpte,---pZ-1 car on est passé dans l'année suivante
				table_filiation_pX,---FROM
				pX_idpar,---pY-1 car on est passé dans l'année suivante
				m1,
				pX_idpar,---pY-1 car on est passé dans l'année suivante
				pY_idpar,---pZ-1 car on est passé dans l'année suivante
				pY_idprocpte---pZ-1 car on est passé dans l'année suivante
				);
			COMMIT;
	
	
			-------------------------------------------
			----NC sans géométrie --> maintenu en NC
			-------------------------------------------
			RAISE NOTICE 'MAINTIEN DU NC SANS GEOMETRIE';
			EXECUTE format(
				\$\$				
				INSERT INTO anaseq.%I(---table_filiation_pY
					ccodep,
					iddep22,
					idcom22,
					annee,
					%I,---idfil_filiation_pX
					%I,---pX_idpar
					%I,---pY_idpar
					%I,---pY_idprocpte
					%I,---pZ_idpar
					area,
					type_evol)
				
				SELECT %L,
					iddep22,
					idcom22,
					annee,
					idpk,
					%I,---pY_idpar--> 'NC'
					'NC',
					'NC',
					'SO',---sans objet
					area,
					'maintien en NC'
				FROM anaseq.%I---table_filiation_pX
				WHERE %I LIKE 'NC'---pY_idpar
					AND geom IS NULL
					AND annee = %L ---m1
				GROUP BY iddep22,
					idcom22,
					annee,
					idpk,
					%I,
					area,
					geom
				\$\$,
				table_filiation_pY,
				idfil_filiation_pX,
				pX_idpar,
				pY_idpar,
				pY_idprocpte,
				pZ_idpar,
				---SELECT
				dep,
				pX_idpar, ---pY-1 car on est passé dans l'année suivante
				table_filiation_pX,---FROM
				pX_idpar,---pY-1 car on est passé dans l'année suivante
				m1,
				pX_idpar---pY-1 car on est passé dans l'année suivante
				);
			COMMIT;
		END IF;

		-------------------------------------------
		----CAS SIMPLES (filiation directe)
		-------------------------------------------
		RAISE NOTICE 'CAS SIMPLES';
	
		EXECUTE format(
			\$\$
			WITH sub AS (
				SELECT pX.iddep22,
					pX.idcom22,
					pX.annee,
					pX.idpk,
					pX.%I,
					pY.idpar %I,
					pY.idprocpte %I,
					pX.area,
					pX.geom,
					'aucune' type_evol
				FROM anaseq.%I pX
				JOIN %I.%I pY ON pX.%I = pY.idpar
					AND pX.%I NOT LIKE 'NC'---à partir de p2 ce cas est possible
				WHERE pX.annee = %L --m1
				UNION
				SELECT pX.iddep22,
					pX.idcom22,
					pX.annee,
					pX.idpk,
					pX.%I,
					pY.idpar %I,
					pY.idprocpte %I,
					pX.area,
					pX.geom,
					'aucune' type_evol
				FROM anaseq.%I pX
				JOIN %I.%I pY ON pX.%I = pY.idpar_inverse
					AND pX.%I NOT LIKE 'NC'---à partir de p2 ce cas est possible
				WHERE pX.annee = %L --m1
			)
			
			INSERT INTO anaseq.%I(---table_filiation_pY
				ccodep,
				iddep22,
				idcom22,
				annee,
				%I,---idfil_filiation_pX
				%I,---pX_idpar
				%I,---pY_idpar
				%I,---pY_idprocpte
				%I,---pZ_idpar
				area,
				geom,
				type_evol)
			
			SELECT %L,
				iddep22,
				idcom22,
				annee,
				idpk,
				%I,---pX_idpar
				%I,---pY_idpar
				%I,---pY_idprocpte
				'SO',---sans objet
				area,
				geom,
				type_evol
			FROM sub
			GROUP BY iddep22,
				idcom22,
				annee,
				idpk,
				%I,
				%I,
				%I,
				area,
				geom,
				type_evol
			\$\$,
			pX_idpar,
			pY_idpar,
			pY_idprocpte,
			table_filiation_pX,
			schema_parcelles_dep_pY, table_parcelles_dep_pY, pX_idpar,
			pX_idpar,
			m1,
			---UNION
			pX_idpar,
			pY_idpar,
			pY_idprocpte,
			table_filiation_pX,
			schema_parcelles_dep_pY, table_parcelles_dep_pY, pX_idpar,
			pX_idpar,
			m1,
			---INSERT
			table_filiation_pY,
			idfil_filiation_pX,
			pX_idpar,
			pY_idpar,
			pY_idprocpte,
			pZ_idpar,
			dep,
			pX_idpar,
			pY_idpar,
			pY_idprocpte,
			pX_idpar,
			pY_idpar,
			pY_idprocpte
			);
		COMMIT;

		-------------------------------------------
		----Sélection des parcelles en pX non-retrouvées en pY à ce stade (dont le NC)
		-------------------------------------------
		EXECUTE format(
			\$\$
			DROP TABLE IF EXISTS anaseq.reste_pX CASCADE;
			CREATE TABLE anaseq.reste_pX AS (
				SELECT pX.iddep22,
					pX.idcom22,
					pX.idpk idfil_pX,
					pX.%I pX_idpar,
					pX.geom pX_geom,
					pX.area pX_area
				FROM anaseq.%I pX---table des parcelles à filier en pX
				LEFT JOIN %I.%I tot_pY ON pX.%I = tot_pY.idpar
				LEFT JOIN %I.%I tot_pYbis ON pX.%I = tot_pYbis.idpar_inverse
				WHERE pX.annee = %L
					AND tot_pY.idpar IS NULL ---qu'on ne retrouve pas en pY, ni en idpar normal
					AND tot_pYbis.idpar_inverse IS NULL ---ni en idpar_inverse
				GROUP BY pX.iddep22,
					pX.idcom22,
					idfil_pX,
					pX_idpar,
					pX_geom,
					pX_area);
			\$\$,
			pX_idpar,
			table_filiation_pX,
			schema_parcelles_dep_pY, table_parcelles_dep_pY, pX_idpar,
			schema_parcelles_dep_pY, table_parcelles_dep_pY, pX_idpar,
			m1);
		COMMIT;

		---Table des parcelles qu'on ne retrouve pas dans les DFI, pour traitement ultérieur (dont le NC)
		EXECUTE format(
			\$\$
			DROP TABLE IF EXISTS anaseq.reste_pX_nodfi CASCADE;
			CREATE TABLE anaseq.reste_pX_nodfi AS (
				SELECT pX.iddep22,
					pX.idcom22,
					pX.idfil_pX,
					pX.pX_idpar,
					pX.pX_geom,
					pX.pX_area
				FROM anaseq.reste_pX pX
				LEFT JOIN dfi.unnest dfi ON pX.pX_idpar = dfi.idpar_mere_normal
					AND EXTRACT(YEAR FROM dfi.date) BETWEEN %s AND %s
					AND pX.pX_idpar NOT LIKE 'NC'
				LEFT JOIN dfi.unnest dfi_bis ON pX.pX_idpar = dfi_bis.idpar_mere_inverse
					AND EXTRACT(YEAR FROM dfi_bis.date) BETWEEN %s AND %s
					AND pX.pX_idpar NOT LIKE 'NC'
				WHERE dfi.idpar_mere_normal IS NULL
					AND dfi_bis.idpar_mere_inverse IS NULL
					AND pX.pX_idpar NOT LIKE 'saut');---on a déjà retrouvé les sauts de millésime
			\$\$,
			pX,pY,
			pX,pY);
		COMMIT;


		-------------------------------------------
		---Traitement des évolutions simples dans les DFI
		-------------------------------------------
		RAISE NOTICE 'EVOLUTIONS SIMPLES';

		---Table de passage entre reste_pX et DFI
		EXECUTE format(
			\$\$
			DROP TABLE IF EXISTS anaseq.reste_pX_dfi CASCADE;
			CREATE TABLE anaseq.reste_pX_dfi AS (
				SELECT dfi.date,
					dfi.id_filiation,
					dfi.type,
					pX.iddep22,
					pX.idcom22,
					pX.idfil_pX,
					pX.pX_idpar, --- parcelles restantes à filier
					pX.pX_geom,
					pX.pX_area,
					dfi.idpar_mere_normal pX_idpar_mere_normal,
					dfi.idpar_fille_normal pX_idpar_fille_normal,
					dfi.idpar_fille_inverse pX_idpar_fille_inverse
				FROM anaseq.reste_pX pX
				JOIN dfi.unnest dfi ON pX.pX_idpar = dfi.idpar_mere_normal---dont on retrouve l'id dans les idpar_mere des DFI
					AND EXTRACT(YEAR FROM date) BETWEEN %s AND %s
					AND pX.pX_idpar NOT LIKE 'NC'
				GROUP BY
					dfi.date,
					dfi.id_filiation,
					dfi.type,
					pX.iddep22,
					pX.idcom22,
					idfil_pX,
					pX_idpar,
					pX_geom,
					pX_area,
					pX_idpar_mere_normal,
					pX_idpar_fille_normal,
					pX_idpar_fille_inverse
				UNION
				SELECT dfi.date,
					dfi.id_filiation,
					dfi.type,
					pX.iddep22,
					pX.idcom22,
					pX.idfil_pX,
					pX.pX_idpar, --- parcelles restantes à filier
					pX.pX_geom,
					pX.pX_area,
					dfi.idpar_mere_normal pX_idpar_mere_normal,
					dfi.idpar_fille_normal pX_idpar_fille_normal,
					dfi.idpar_fille_inverse pX_idpar_fille_inverse
				FROM anaseq.reste_pX pX
				JOIN dfi.unnest dfi ON pX.pX_idpar = dfi.idpar_mere_inverse---dont on retrouve l'id dans les idpar_mere inverse des DFI
					AND EXTRACT(YEAR FROM date) BETWEEN %s AND %s
				GROUP BY
					dfi.date,
					dfi.id_filiation,
					dfi.type,
					pX.iddep22,
					pX.idcom22,
					idfil_pX,
					pX_idpar,
					pX_geom,
					pX_area,
					pX_idpar_mere_normal,
					pX_idpar_fille_normal,
					pX_idpar_fille_inverse);
			DROP TABLE IF EXISTS anaseq.reste_pX CASCADE;
			\$\$,
			pX,pY,
			pX,pY);
		COMMIT;

		---Insert de la filiation si pY = NC (parcelle devenue du NC)
		EXECUTE format(
			\$\$
			INSERT INTO anaseq.%I(
				ccodep,
				iddep22,
				idcom22,
				annee,
				%I,---idfil_pX
				%I,---idpar_pX
				%I,---idpar_pY
				%I,---idprocpte_pY
				%I,---idpar_pZ
				area,
				geom,
				type_evol)
			
			SELECT %L,
				iddep22,
				idcom22,
				%L,---annee d'évolution initiale
				idfil_pX,
				pX_idpar,
				pX_idpar_fille_normal,----NC
				pX_idpar_fille_normal,----NC
				'SO',---sans objet
				pX_area,
				pX_geom,
				'passage en NC'
			FROM anaseq.reste_pX_dfi
			WHERE pX_idpar_fille_normal LIKE 'NC'
			GROUP BY iddep22,
				idcom22,
				idfil_pX,
				pX_idpar,
				pX_area,
				pX_geom,
				pX_idpar_fille_normal;

			DELETE FROM anaseq.reste_pX_dfi WHERE pX_idpar_fille_normal LIKE 'NC';
			\$\$,
			table_filiation_pY,
			idfil_filiation_pX,
			pX_idpar,
			pY_idpar,
			pY_idprocpte,
			pZ_idpar,
			dep,
			m1);
		COMMIT;

		---Recherche de parcelles-mères correspondantes dans les Fichiers Fonciers en pY pour les autres
		EXECUTE format(
			\$\$
			DROP TABLE IF EXISTS anaseq.reste_pX_dfi_tot_pY CASCADE;
			CREATE TABLE anaseq.reste_pX_dfi_tot_pY AS (
				SELECT pX.id_filiation,
					pX.date,
					pX.type type_evol,
					pX.iddep22,
					pX.idcom22,
					pX.idfil_pX,
					pX.pX_idpar,
					pX.pX_geom,
					pX.pX_area,
					pX.pX_idpar_mere_normal,
					pX_idpar_fille_normal,
					pX_idpar_fille_inverse,
					CASE
						WHEN tot_pY.idpar IS NULL AND tot_pYbis.idpar IS NULL THEN tot_pYter.idpar
						WHEN tot_pY.idpar IS NULL AND tot_pYbis.idpar IS NOT NULL THEN tot_pYbis.idpar
						ELSE tot_pY.idpar
					END pY_idpar_tot_pY,
					CASE
						WHEN tot_pY.idpar IS NULL AND tot_pYbis.idpar IS NULL THEN tot_pYter.dcntpa
						WHEN tot_pY.idpar IS NULL AND tot_pYbis.idpar IS NOT NULL THEN tot_pYbis.dcntpa
						ELSE tot_pY.dcntpa
					END pY_area_tot_pY,
					CASE
						WHEN tot_pY.idpar IS NULL AND tot_pYbis.idpar IS NULL THEN tot_pYter.idprocpte
						WHEN tot_pY.idpar IS NULL AND tot_pYbis.idpar IS NOT NULL THEN tot_pYbis.idprocpte
						ELSE tot_pY.idprocpte
					END pY_idprocpte_tot_pY
				FROM anaseq.reste_pX_dfi pX
				LEFT JOIN %I.%I tot_pY ON pX.pX_idpar_fille_normal = tot_pY.idpar---on regarde si on retrouve les idparfille dans les FF ou non
				LEFT JOIN %I.%I tot_pYbis ON pX.pX_idpar_fille_normal = tot_pYbis.idpar_inverse---on regarde si on retrouve les idparfille dans les FF ou non
				LEFT JOIN %I.%I tot_pYter ON pX.pX_idpar_fille_inverse = tot_pYter.idpar---on regarde si on retrouve les idparfille dans les FF ou non
				GROUP BY pX.id_filiation,
					pX.date,
					pX.type,
					pX.iddep22,
					pX.idcom22,
					idfil_pX,
					pX_idpar,
					pX_geom,
					pX_area,
					pX.pX_idpar_mere_normal,
					pX_idpar_fille_normal,
					pX_idpar_fille_inverse,
					pY_idpar_tot_pY,
					pY_area_tot_pY,
					pY_idprocpte_tot_pY);
			DROP TABLE IF EXISTS anaseq.reste_pX_dfi CASCADE;
			\$\$,
			schema_parcelles_dep_pY, table_parcelles_dep_pY,
			schema_parcelles_dep_pY, table_parcelles_dep_pY,
			schema_parcelles_dep_pY, table_parcelles_dep_pY);
		COMMIT;

		---Table de passage géométrique temporaire pX_pY
		EXECUTE format(
			\$\$
			DROP TABLE IF EXISTS anaseq.passage_pX_pY CASCADE;
			CREATE TABLE anaseq.passage_pX_pY(
					iddep22 varchar(2),
					idcom22 varchar(5),
					idfil_pX int,
					pX_idpar text,
					pX_geom geometry,
					pX_area numeric,
					pY_idpar text,
					pY_area numeric,
					pY_idprocpte text,
					type_evol text);
			\$\$);
		COMMIT;

		---Insert dans la table de passage géométrique si filiation entièrement retrouvée en évolution simple
		EXECUTE format(
			\$\$				
			WITH sub AS (
				SELECT pX.pX_idpar,
					COUNT(*) FILTER (WHERE pY_idpar_tot_pY IS NULL) as nb_par_manquantes
				FROM anaseq.reste_pX_dfi_tot_pY pX
				GROUP BY pX.pX_idpar),
						
			sub2 AS (
				SELECT iddep22,
					idcom22,
					pX.idfil_pX,
					pX.pX_idpar,
					pX_geom,
					pX_area,
					pY_idpar_tot_pY pY_idpar,
					pY_area_tot_pY pY_area,
					pY_idprocpte_tot_pY pY_idprocpte,
					type_evol
				FROM anaseq.reste_pX_dfi_tot_pY pX
				JOIN sub ON sub.pX_idpar = pX.pX_idpar
					AND sub.nb_par_manquantes = 0
				GROUP BY iddep22,
					idcom22,
					idfil_pX,
					pX.pX_idpar,
					pX_geom,
					pX_area,
					pY_idpar,
					pY_area,
					pY_idprocpte,
					type_evol)

			INSERT INTO anaseq.passage_pX_pY
				SELECT iddep22,
					idcom22,
					idfil_pX,
					pX_idpar,
					pX_geom,
					pX_area,
					pY_idpar,
					pY_area,
					pY_idprocpte,
					type_evol
				FROM sub2
				GROUP BY iddep22,
					idcom22,
					idfil_pX,
					pX_idpar,
					pX_geom,
					pX_area,
					pY_idpar,
					pY_area,
					pY_idprocpte,
					type_evol
			\$\$);
		COMMIT;

		---On supprime des parcelles à filier toutes celles qu'on a retrouvées entièrement en évolution simple
		EXECUTE format(
			\$\$
			DELETE FROM anaseq.reste_pX_dfi_tot_pY 
			WHERE pX_idpar IN
				(SELECT pX_idpar
				FROM anaseq.reste_pX_dfi_tot_pY 
				GROUP BY pX_idpar
				HAVING COUNT(*) FILTER (WHERE pY_idpar_tot_pY IS NULL) = 0);
			\$\$);
		COMMIT;

		
		-------------------------------------------
		---Traitement des évolutions complexes
		-------------------------------------------
		RAISE NOTICE 'EVOLUTIONS COMPLEXES';

		---Table à 3 niveaux (trop lourd au-delà et peu d'impact) des filiations sur 1 à 3 DFI consécutifs
		EXECUTE format(
			\$\$
			DROP TABLE IF EXISTS anaseq.pX_complexe CASCADE;
			CREATE TABLE anaseq.pX_complexe AS (
				WITH sub AS (
					SELECT pX.iddep22,
						pX.idcom22,
						pX.date date1,
						dfi.date date2,
						pX.idfil_pX,
						pX.pX_idpar idpar0,
						pX.pX_geom geom0,
						pX.pX_area area0,
						pX.pX_idpar_fille_normal idpar1,
						pX.pY_idpar_tot_pY idpar1_tot_pY,
						pX.pY_area_tot_pY area1_tot_pY,
						pX.pY_idprocpte_tot_pY idprocpte1_tot_pY,
						dfi.idpar_fille_normal idpar2,---on garde les idpar fille de 2e niveau
						CASE
							WHEN tot_pY.idpar IS NULL AND tot_pYbis.idpar IS NULL THEN tot_pYter.idpar
							WHEN tot_pY.idpar IS NULL AND tot_pYbis.idpar IS NOT NULL THEN tot_pYbis.idpar
							ELSE tot_pY.idpar
						END idpar2_tot_pY,
						CASE
							WHEN tot_pY.idpar IS NULL AND tot_pYbis.idpar IS NULL THEN tot_pYter.dcntpa
							WHEN tot_pY.idpar IS NULL AND tot_pYbis.idpar IS NOT NULL THEN tot_pYbis.dcntpa
							ELSE tot_pY.dcntpa
						END area2_tot_pY,
						CASE
							WHEN tot_pY.idpar IS NULL AND tot_pYbis.idpar IS NULL THEN tot_pYter.idprocpte
							WHEN tot_pY.idpar IS NULL AND tot_pYbis.idpar IS NOT NULL THEN tot_pYbis.idprocpte
							ELSE tot_pY.idprocpte
						END idprocpte2_tot_pY
					FROM anaseq.reste_pX_dfi_tot_pY pX
					LEFT JOIN dfi.unnest dfi ON dfi.idpar_mere_normal = pX.pX_idpar_fille_normal
						AND pX.pY_idpar_tot_pY IS NULL
						AND dfi.date >= pX.date---seulement si la filiation est postérieure
					LEFT JOIN %I.%I tot_pY ON dfi.idpar_fille_normal = tot_pY.idpar---on check si on retrouve une parcelle en pY
						AND pX.pY_idpar_tot_pY IS NULL---jointure uniquement si géométrie pas déjà retrouvée
						AND dfi.idpar_fille_normal NOT LIKE 'NC' --- sauf si devient du non-cadastré
					LEFT JOIN %I.%I tot_pYbis ON dfi.idpar_fille_normal = tot_pYbis.idpar_inverse---on regarde si on retrouve les idparmere dans les parcelles de pX ou non
						AND pX.pY_idpar_tot_pY IS NULL
						AND dfi.idpar_fille_normal NOT LIKE 'NC' --- sauf si devient du non-cadastré
					LEFT JOIN %I.%I tot_pYter ON dfi.idpar_fille_inverse = tot_pYter.idpar
						AND pX.pY_idpar_tot_pY IS NULL
						AND dfi.idpar_fille_normal NOT LIKE 'NC' --- sauf si devient du non-cadastré
					GROUP BY date1,
						date2,
						pX.iddep22,
						pX.idcom22,
						idfil_pX,
						idpar0,
						geom0,
						area0,
						idpar1,
						idpar1_tot_pY,
						area1_tot_pY,
						idprocpte1_tot_pY,
						idpar2,
						idpar2_tot_pY,
						area2_tot_pY,
						idprocpte2_tot_pY)
				
				SELECT sub.date1,
					sub.date2,
					dfi.date date3,
					sub.iddep22,
					sub.idcom22,
					sub.idfil_pX,
					sub.idpar0,
					sub.geom0,
					sub.area0,
					sub.idpar1,
					sub.idpar1_tot_pY,
					sub.area1_tot_pY,
					sub.idprocpte1_tot_pY,
					sub.idpar2,
					sub.idpar2_tot_pY,
					sub.area2_tot_pY,
					sub.idprocpte2_tot_pY,
					dfi.idpar_fille_normal idpar3,----- et les idpar_fille de 3e niveau
					CASE
						WHEN tot_pY.idpar IS NULL AND tot_pYbis.idpar IS NULL THEN tot_pYter.idpar
						WHEN tot_pY.idpar IS NULL AND tot_pYbis.idpar IS NOT NULL THEN tot_pYbis.idpar
						ELSE tot_pY.idpar
					END idpar3_tot_pY,
					CASE
						WHEN tot_pY.idpar IS NULL AND tot_pYbis.idpar IS NULL THEN tot_pYter.dcntpa
						WHEN tot_pY.idpar IS NULL AND tot_pYbis.idpar IS NOT NULL THEN tot_pYbis.dcntpa
						ELSE tot_pY.dcntpa
					END area3_tot_pY,
					CASE
						WHEN tot_pY.idpar IS NULL AND tot_pYbis.idpar IS NULL THEN tot_pYter.idprocpte
						WHEN tot_pY.idpar IS NULL AND tot_pYbis.idpar IS NOT NULL THEN tot_pYbis.idprocpte
						ELSE tot_pY.idprocpte
					END idprocpte3_tot_pY
				FROM sub
				LEFT JOIN dfi.unnest dfi ON dfi.idpar_mere_normal = sub.idpar2
					AND dfi.date >= sub.date2---seulement si la filiation est postérieure
					AND sub.idpar2_tot_pY IS NULL
					AND sub.idpar2 NOT LIKE 'NC'---pas de filiation supplémentaire dans les DFI si devient du NC
				LEFT JOIN %I.%I tot_pY ON dfi.idpar_fille_normal = tot_pY.idpar---on check si on retrouve une parcelle en pY
					AND sub.idpar2_tot_pY IS NULL---jointure uniquement si géométrie pas déjà retrouvée
					AND dfi.idpar_fille_normal NOT LIKE 'NC' --- sauf si devient du non-cadastré
				LEFT JOIN %I.%I tot_pYbis ON dfi.idpar_fille_normal = tot_pYbis.idpar_inverse---on regarde si on retrouve les idparfille dans les parcelles de 2018 ou non
					AND dfi.idpar_fille_normal NOT LIKE 'NC' --- sauf si devient du non-cadastré
				LEFT JOIN %I.%I tot_pYter ON dfi.idpar_fille_inverse = tot_pYter.idpar
					AND sub.idpar2_tot_pY IS NULL
					AND dfi.idpar_fille_normal NOT LIKE 'NC' --- sauf si devient du non-cadastré
				GROUP BY date1,
					date2,
					date3,
					sub.iddep22,
					sub.idcom22,
					sub.idfil_pX,
					idpar0,
					geom0,
					area0,
					idpar1,
					idpar1_tot_pY,
					area1_tot_pY,
					idprocpte1_tot_pY,
					idpar2,
					idpar2_tot_pY,
					area2_tot_pY,
					idprocpte2_tot_pY,
					idpar3,
					idpar3_tot_pY,
					area3_tot_pY,
					idprocpte3_tot_pY
				);
			DROP TABLE anaseq.reste_pX_dfi_tot_pY CASCADE;
			\$\$,
			schema_parcelles_dep_pY, table_parcelles_dep_pY,
			schema_parcelles_dep_pY, table_parcelles_dep_pY,
			schema_parcelles_dep_pY, table_parcelles_dep_pY,
			schema_parcelles_dep_pY, table_parcelles_dep_pY,
			schema_parcelles_dep_pY, table_parcelles_dep_pY,
			schema_parcelles_dep_pY, table_parcelles_dep_pY);
		COMMIT;

		---Extraction et élagage des filiations complexes qu'on retrouve dans les Fichiers Fonciers ou en NC
		EXECUTE format(
			\$\$
			DROP TABLE IF EXISTS anaseq.pX_complexe_ff CASCADE;
			CREATE TABLE anaseq.pX_complexe_ff AS (
				SELECT iddep22,
					idcom22,
					idfil_pX,
					idpar0,
					geom0,
					area0,
					CASE
						WHEN idpar1 LIKE 'NC'
							OR idpar2 LIKE 'NC'
							OR idpar3 LIKE 'NC'
								THEN 'NC'
						WHEN idpar3_tot_pY IS NOT NULL THEN idpar3_tot_pY
						WHEN idpar2_tot_pY IS NOT NULL THEN idpar2_tot_pY
						WHEN idpar1_tot_pY IS NOT NULL THEN idpar1_tot_pY
						ELSE NULL
					END idpar_new,
					CASE
						WHEN idpar1 LIKE 'NC'
							OR idpar2 LIKE 'NC'
							OR idpar3 LIKE 'NC'
								THEN 'NC'
						WHEN idpar3_tot_pY IS NOT NULL THEN idprocpte3_tot_pY
						WHEN idpar2_tot_pY IS NOT NULL THEN idprocpte2_tot_pY
						WHEN idpar1_tot_pY IS NOT NULL THEN idprocpte1_tot_pY
						ELSE NULL
					END idprocpte_new,
					CASE
						WHEN idpar1 LIKE 'NC'
							OR idpar2 LIKE 'NC'
							OR idpar3 LIKE 'NC'
								THEN area0
						WHEN idpar3_tot_pY IS NOT NULL THEN area3_tot_pY
						WHEN idpar2_tot_pY IS NOT NULL THEN area2_tot_pY
						WHEN idpar1_tot_pY IS NOT NULL THEN area1_tot_pY
						ELSE NULL
					END area_new
				FROM anaseq.pX_complexe);
			\$\$);
		COMMIT;

		---Insert dans la table de filiation des évolutions complexes qui deviennent du NC
		EXECUTE format(
			\$\$
			INSERT INTO anaseq.%I(
				ccodep,
				iddep22,
				idcom22,
				annee,
				%I,---idfil_pX
				%I,---idpar_pX
				%I,---idpar_pY
				%I,---idprocpte_pY
				%I,---idpar_pZ
				area,
				geom,
				type_evol)
			
			SELECT %L,
				iddep22,
				idcom22,
				%L,---annee d'évolution initiale
				idfil_pX,
				idpar0,
				idpar_new,----NC
				'NC',
				'SO',---sans objet
				area0,
				geom0,
				'passage en NC complexe'
			FROM anaseq.pX_complexe_ff
			WHERE idpar_new LIKE 'NC'
			GROUP BY iddep22,
				idcom22,
				idfil_pX,
				idpar0,
				idpar_new,
				area0,
				geom0;

			DELETE FROM anaseq.pX_complexe comp
			USING anaseq.pX_complexe_ff ff
			WHERE ff.idpar0 = comp.idpar0
				AND ff.idpar_new LIKE 'NC';
			\$\$,
			table_filiation_pY,
			idfil_filiation_pX,
			pX_idpar,
			pY_idpar,
			pY_idprocpte,
			pZ_idpar,
			dep,
			m1);
		COMMIT;

		---Insert dans la table de passage géométrique temporaire des autres évolutions pX_pY
		EXECUTE format(
			\$\$			
			INSERT INTO anaseq.passage_pX_pY
				SELECT comp.iddep22,
					comp.idcom22,
					comp.idfil_pX,
					comp.idpar0,
					comp.geom0,
					comp.area0,
					comp.idpar_new,
					comp.area_new,
					comp.idprocpte_new,
					'évolution complexe' type_evol
				FROM anaseq.pX_complexe_ff comp
				WHERE idpar_new NOT LIKE 'NC'
				GROUP BY comp.iddep22,
					comp.idcom22,
					comp.idfil_pX,
					comp.idpar0,
					comp.geom0,
					comp.area0,
					comp.idpar_new,
					comp.area_new,
					comp.idprocpte_new;
			\$\$);
		COMMIT;


		--------------------
		---Insert des évolutions simples et complexes depuis la table de passage géométrique
		--------------------
		RAISE NOTICE 'FIABILISATION DES GEOMETRIES ET INSERT DES FILIATIONS';

		---Recherche des dernières géométries parcellaires dans la table de passage pX_pY
		EXECUTE format(
			\$\$
			---Création des colonnes et index
			ALTER TABLE anaseq.passage_pX_pY DROP COLUMN IF EXISTS pY_lastgeompar CASCADE;
			ALTER TABLE anaseq.passage_pX_pY ADD COLUMN pY_lastgeompar geometry;
			CREATE INDEX pX_passage_geom_idx ON anaseq.passage_pX_pY USING gist(pX_geom);
			CREATE INDEX pY_lastgeompar_idx ON anaseq.passage_pX_pY USING gist(pY_lastgeompar);
			CREATE INDEX pX_passage_idpar_new_idx ON anaseq.passage_pX_pY(pY_idpar);
			\$\$);

		FOREACH millesime_de_reference IN ARRAY ARRAY['2021','2020','2019','2018','2017','2016','2015','2014','2013','2012','2011','2009']
		LOOP
			schema_de_reference := 'ff_'||millesime_de_reference||'_dep';
	
			IF millesime_de_reference::numeric < 2018 THEN
					table_de_reference2 := 'd'||dep||'_'||millesime_de_reference||'_pnb10_parcelle';
			ELSE
					table_de_reference2 := 'd'||dep||'_fftp_'||millesime_de_reference||'_pnb10_parcelle';
			END IF;
			table_de_reference := table_de_reference2;

			IF millesime_de_reference::numeric >= pY::numeric-1 THEN---On ne recherche pas lastgeompar plus d'un an avant l'année considérée
	
					EXECUTE format(
						\$\$		
						UPDATE anaseq.passage_pX_pY a
						SET pY_lastgeompar = ST_MakeValid(b.geompar)
						FROM %I.%I b
						WHERE a.pY_lastgeompar IS NULL
							AND b.geompar IS NOT NULL
							AND a.pY_idpar = b.idpar
							AND b.vecteur LIKE 'V';
		
						UPDATE anaseq.passage_pX_pY a
						SET pY_lastgeompar = ST_MakeValid(b.geompar)
						FROM %I.%I b
						WHERE a.pY_lastgeompar IS NULL
							AND b.geompar IS NOT NULL
							AND a.pY_idpar = b.idpar_inverse---si changement communal
							AND b.vecteur LIKE 'V';
						\$\$,
						schema_de_reference, table_de_reference,
						schema_de_reference, table_de_reference);
		
			END IF;
		END LOOP;
		COMMIT;

		---Insert des filiations où lastgeompar est retrouvée et valide
		EXECUTE format(
			\$\$
			INSERT INTO anaseq.%I(
				ccodep,
				iddep22,
				idcom22,
				annee,
				%I,---idfil_pX
				%I,---idpar_pX
				%I,---idpar_pY
				%I,---idprocpte_pY
				%I,---idpar_pZ
				geom,
				type_evol
				)
			SELECT %L,
				sub.iddep22,
				sub.idcom22,
				%L,---année d'évolution initiale
				sub.idfil_pX,
				sub.pX_idpar,
				sub.pY_idpar,
				sub.pY_idprocpte,
				'SO',---sans objet en pZ
				ST_Union(ST_CollectionExtract(ST_MakeValid(ST_Intersection(sub.pX_geom,sub.pY_lastgeompar)),3)),
				type_evol	
			FROM anaseq.passage_pX_pY sub
			WHERE sub.pX_geom IS NOT NULL
				AND sub.pY_lastgeompar IS NOT NULL
			GROUP BY sub.iddep22,
				sub.idcom22,
				sub.idfil_pX,
				sub.pX_idpar,
				sub.pY_idpar,
				sub.pY_idprocpte,
				sub.type_evol;
			UPDATE anaseq.%I SET area = ST_Area(geom) WHERE area IS NULL;
			\$\$,
			table_filiation_pY,
			idfil_filiation_pX,
			pX_idpar,
			pY_idpar,
			pY_idprocpte,
			pZ_idpar,
			dep,
			m1,
			table_filiation_pY);
		COMMIT;

		
		BEGIN---en cas d'erreur de géométrie
		
			---Insert des non-superpositions géométriques dans la table nodfi
			EXECUTE format(
				\$\$
				DROP TABLE IF EXISTS anaseq.fil_pY_uni CASCADE;
				CREATE TABLE anaseq.fil_pY_uni AS (
					SELECT ST_Union(ST_MakeValid(ST_CollectionExtract(geom,3))) geom
					FROM anaseq.%I
					WHERE annee = %L
						AND type_evol NOT LIKE 'aucune'
						AND type_evol NOT LIKE 'passage%%'
						AND type_evol NOT LIKE 'saut%%'
						AND type_evol NOT LIKE 'maintien%%'
				);
				CREATE INDEX fil_pY_uni_geom_idx ON anaseq.fil_pY_uni USING gist(geom);
				
				DROP TABLE IF EXISTS anaseq.passuni CASCADE;
				CREATE TABLE anaseq.passuni AS (
					SELECT ST_Union(ST_MakeValid(ST_CollectionExtract(pX_geom,3))) geom
					FROM anaseq.passage_pX_pY
					WHERE pX_geom IS NOT NULL
						AND pY_lastgeompar IS NOT NULL
				);
				CREATE INDEX passuni_geom_idx ON anaseq.passuni USING gist(geom);
				
				DROP TABLE IF EXISTS anaseq.diff CASCADE;
				CREATE TABLE anaseq.diff AS (
					SELECT ST_MakeValid(ST_CollectionExtract(ST_Difference(passuni.geom, fil_pY_uni.geom),3)) geom
					FROM anaseq.fil_pY_uni, anaseq.passuni	
				);
				
				CREATE INDEX diff_geom_idx ON anaseq.diff USING gist(geom);
				\$\$,
				table_filiation_pY,
				m1
			);

		EXCEPTION----En cas d'erreur de différence spatiale, on refait avec ST_MemUnion sur la table anaseq.fil_mY_uni
			WHEN OTHERS THEN
				
				EXECUTE format(
				\$\$
				DROP TABLE IF EXISTS anaseq.fil_pY_uni CASCADE;
				CREATE TABLE anaseq.fil_pY_uni AS (
					SELECT ST_MemUnion(ST_MakeValid(ST_CollectionExtract(geom,3))) geom
					FROM anaseq.%I
					WHERE annee = %L
						AND type_evol NOT LIKE 'aucune'
						AND type_evol NOT LIKE 'passage%%'
						AND type_evol NOT LIKE 'saut%%'
						AND type_evol NOT LIKE 'maintien%%'
				);
				CREATE INDEX fil_pY_uni_geom_idx ON anaseq.fil_pY_uni USING gist(geom);
				
				DROP TABLE IF EXISTS anaseq.passuni CASCADE;
				CREATE TABLE anaseq.passuni AS (
					SELECT ST_Union(ST_MakeValid(ST_CollectionExtract(pX_geom,3))) geom
					FROM anaseq.passage_pX_pY
					WHERE pX_geom IS NOT NULL
						AND pY_lastgeompar IS NOT NULL
				);
				CREATE INDEX passuni_geom_idx ON anaseq.passuni USING gist(geom);
				
				CREATE TABLE anaseq.diff AS (
					SELECT ST_MakeValid(ST_CollectionExtract(ST_Difference(passuni.geom, fil_pY_uni.geom),3)) geom
					FROM anaseq.fil_pY_uni, anaseq.passuni	
				);
				CREATE INDEX diff_geom_idx ON anaseq.diff USING gist(geom);
				\$\$,
				table_filiation_pY,
				m1);
			
		END;
	

		EXECUTE format(
			\$\$	
			INSERT INTO anaseq.reste_pX_nodfi(
				iddep22,
				idcom22,
				idfil_pX,
				pX_idpar,
				pX_geom)
			SELECT pass.iddep22,
				pass.idcom22,
				pass.idfil_pX,
				pass.pX_idpar,
				ST_Union(ST_MakeValid(ST_CollectionExtract(ST_Intersection(diff.geom,pass.pX_geom))),3)
			FROM anaseq.passage_pX_pY pass, anaseq.diff
			WHERE pass.pX_geom IS NOT NULL
			GROUP BY pass.iddep22,
				pass.idcom22,
				pass.idfil_pX,
				pass.pX_idpar;
			
			UPDATE anaseq.reste_pX_nodfi
			SET pX_area = ST_Area(pX_geom)
			WHERE pX_area IS NULL
				AND pX_geom IS NOT NULL;

			DROP TABLE anaseq.fil_pY_uni CASCADE;
			DROP TABLE anaseq.passuni CASCADE;
			DROP TABLE anaseq.diff CASCADE;
			\$\$);
		COMMIT;

		---On compte pour chaque parcelle en mX la surface non-retrouvée géométriquement
		---> pour distibuter équitablement la surface restante entre les parcelles filles/mères
		EXECUTE format(
			\$\$
			ALTER TABLE anaseq.passage_pX_pY ADD COLUMN surf_geom_manquante numeric;

			WITH sub AS(
				SELECT %I,
					SUM(area) area_retrouvee
				FROM anaseq.%I
				WHERE annee = %L
				GROUP BY %I
			)
			UPDATE anaseq.passage_pX_pY a
			SET surf_geom_manquante = COALESCE(COALESCE(a.pX_area,0)-COALESCE(sub.area_retrouvee,0),0)
			FROM sub
			WHERE a.pX_idpar = sub.%I;

			UPDATE anaseq.passage_pX_pY
			SET surf_geom_manquante = pX_area
			WHERE surf_geom_manquante IS NULL
			\$\$,
			pX_idpar,
			table_filiation_pY,
			m1,
			pX_idpar,
			pX_idpar);
		COMMIT;

		---Table des parcelles avec lacune géométrique
		EXECUTE format(
			\$\$
			DROP TABLE IF EXISTS anaseq.passage_pX_pY_lacunesgeom CASCADE;
			CREATE TABLE anaseq.passage_pX_pY_lacunesgeom AS (
				SELECT * FROM anaseq.passage_pX_pY
				WHERE pX_geom IS NULL
					OR pY_lastgeompar IS NULL);
			\$\$);
		COMMIT;

		---Calcul du nombre de mères et de filles
		EXECUTE format(
			\$\$
			ALTER TABLE anaseq.passage_pX_pY_lacunesgeom ADD COLUMN nb_meres int;
			ALTER TABLE anaseq.passage_pX_pY_lacunesgeom ADD COLUMN nb_filles int;

			WITH sub AS (
				SELECT pX_idpar, COUNT(DISTINCT pY_idpar) nb_filles
				FROM anaseq.passage_pX_pY_lacunesgeom
				GROUP BY pX_idpar)
			UPDATE anaseq.passage_pX_pY_lacunesgeom a
			SET nb_filles = sub.nb_filles
			FROM sub
			WHERE a.pX_idpar = sub.pX_idpar;

			WITH sub AS (
				SELECT pY_idpar, COUNT(DISTINCT pX_idpar) nb_meres
				FROM anaseq.passage_pX_pY_lacunesgeom
				GROUP BY pY_idpar			
			)
			UPDATE anaseq.passage_pX_pY_lacunesgeom a
			SET nb_meres = sub.nb_meres
			FROM sub
			WHERE a.pY_idpar = sub.pY_idpar;
			\$\$);
		COMMIT;

		---Insert de ces filiations si lastgeompar non-retrouvée
		EXECUTE format(
			\$\$
			INSERT INTO anaseq.%I(
				ccodep,
				iddep22,
				idcom22,
				annee,
				%I,---idfil_pX
				%I,---idpar_pX
				%I,---idpar_pY
				%I,---idprocpte_pY
				%I,---idpar_pZ
				geom,
				area,
				type_evol
				)
			SELECT %L,
				iddep22,
				idcom22,
				%L,---année d'évolution initiale
				idfil_pX,
				pX_idpar,
				pY_idpar,
				pY_idprocpte,
				'SO',---sans objet en pZ
				CASE
					WHEN type_evol LIKE 'fusion'
						OR type_evol LIKE 'transfert'
						OR (nb_meres >= 1 AND nb_filles = 1)
							THEN pX_geom
					ELSE NULL::geometry
				END,
				CASE
					WHEN type_evol LIKE 'fusion'
						OR type_evol LIKE 'transfert'
						OR (nb_meres >= 1 AND nb_filles = 1)
							THEN pX_area
					ELSE surf_geom_manquante/(nb_meres+nb_filles-1)---divise la surface par le nombre de portions
				END,
				CONCAT(type_evol, ' (avec géométrie invalide)')
			FROM anaseq.passage_pX_pY_lacunesgeom sub
			WHERE pX_geom IS NULL
				OR pY_lastgeompar IS NULL
			GROUP BY iddep22,
				idcom22,
				idfil_pX,
				pX_idpar,
				pY_idpar,
				pY_idprocpte,
				nb_meres,
				nb_filles,
				pX_geom,
				pX_area,
				surf_geom_manquante,
				type_evol;
			\$\$,
			table_filiation_pY,
			idfil_filiation_pX,
			pX_idpar,
			pY_idpar,
			pY_idprocpte,
			pZ_idpar,
			dep,
			m1);
		COMMIT;

		---Suppression des lignes reconstituées par filiation complexe
		EXECUTE format(
			\$\$
			DELETE FROM anaseq.pX_complexe comp
			USING anaseq.passage_pX_pY pass
			WHERE comp.idpar0 = pass.pX_idpar AND
				(comp.idpar1 = pass.pY_idpar
				OR comp.idpar2 = pass.pY_idpar
				OR comp.idpar3 = pass.pY_idpar);
			
			DROP TABLE anaseq.pX_complexe_ff CASCADE;
			DROP TABLE anaseq.passage_pX_pY CASCADE;
			DROP TABLE anaseq.passage_pX_pY_lacunesgeom CASCADE;
			\$\$);
		COMMIT;



		IF X >= 4 THEN---pas de saut de millésime au-delà de 2021
			EXECUTE format(
				\$\$				
				INSERT INTO anaseq.reste_pX_nodfi
					SELECT iddep22,
						idcom22,
						idfil_pX,
						idpar0,
						geom0,
						area0
					FROM anaseq.pX_complexe;
				DROP TABLE anaseq.pX_complexe CASCADE;
				\$\$);
			COMMIT;
		
		ELSE
			-------------------------------------------
			---Traitement des sauts de millésime pour les parcelles restantes
			-------------------------------------------
			RAISE NOTICE 'SAUTS DE MILLESIME';
			
			----Sélection des parcelles dont on retrouve l identifiant en pZ
			EXECUTE format(
				\$\$
				DROP TABLE IF EXISTS anaseq.pX_sauts_millesimes CASCADE;
				CREATE TABLE anaseq.pX_sauts_millesimes AS (
					
					----sans changement d'idpar
					WITH sub1 AS (
						SELECT comp.iddep22,
							comp.idcom22,
							comp.idfil_pX,
							comp.idpar0,
							comp.geom0,
							comp.area0,
							comp.idpar1,
							comp.idpar2,
							comp.idpar3,
							CASE
								WHEN tot_pZ.idpar IS NULL AND tot_pZbis.idpar IS NOT NULL THEN tot_pZbis.idpar
								ELSE tot_pZ.idpar
							END idpar0_tot_pZ,
							CASE
								WHEN tot_pZ.idpar IS NULL AND tot_pZbis.idpar IS NOT NULL THEN tot_pZbis.dcntpa
								ELSE tot_pZ.dcntpa
							END area0_tot_pZ,
							CASE
								WHEN tot_pZ.idpar IS NULL AND tot_pZbis.idpar IS NOT NULL THEN tot_pZbis.idprocpte
								ELSE tot_pZ.idprocpte
							END idprocpte0_tot_pZ,
							'saut de millésime sans évolution' type_evol
						FROM anaseq.pX_complexe comp
						LEFT JOIN %I.%I tot_pZ ON comp.idpar0 = tot_pZ.idpar---on check si on retrouve une parcelle en pZ
						LEFT JOIN %I.%I tot_pZbis ON comp.idpar0= tot_pZbis.idpar_inverse),---on regarde si on retrouve les idparfille dans les parcelles de pZ ou non
					
					----+niveau1
					sub2 AS (
						SELECT sub1.iddep22,
							sub1.idcom22,
							sub1.idfil_pX,
							sub1.idpar0,
							sub1.geom0,
							sub1.area0,
							sub1.idpar1,
							sub1.idpar2,
							sub1.idpar3,
							sub1.idpar0_tot_pZ,
							sub1.area0_tot_pZ,
							sub1.idprocpte0_tot_pZ,
							CASE
								WHEN tot_pZ.idpar IS NULL AND tot_pZbis.idpar IS NOT NULL THEN tot_pZbis.idpar
								ELSE tot_pZ.idpar
							END idpar1_tot_pZ,
							CASE
								WHEN tot_pZ.idpar IS NULL AND tot_pZbis.idpar IS NOT NULL THEN tot_pZbis.dcntpa
								ELSE tot_pZ.dcntpa
							END area1_tot_pZ,
							CASE
								WHEN tot_pZ.idpar IS NULL AND tot_pZbis.idpar IS NOT NULL THEN tot_pZbis.idprocpte
								ELSE tot_pZ.idprocpte
							END idprocpte1_tot_pZ,
							'évolution simple avec saut de millésime' type_evol
						FROM sub1
						LEFT JOIN %I.%I tot_pZ ON sub1.idpar1 = tot_pZ.idpar---on check si on retrouve une parcelle en pZ
							AND sub1.idpar0_tot_pZ IS NULL---seulement si la géométrie n'a pas déjà été retrouvée
						LEFT JOIN %I.%I tot_pZbis ON sub1.idpar1= tot_pZbis.idpar_inverse---on regarde si on retrouve les idparfille dans les parcelles de pZ ou non
							AND sub1.idpar0_tot_pZ IS NULL),
					----+niveau2
					sub3 AS (
						SELECT sub2.iddep22,
							sub2.idcom22,
							sub2.idfil_pX,
							sub2.idpar0,
							sub2.geom0,
							sub2.area0,
							sub2.idpar1,
							sub2.idpar2,
							sub2.idpar3,
							sub2.idpar0_tot_pZ,
							sub2.area0_tot_pZ,
							sub2.idprocpte0_tot_pZ,
							sub2.idpar1_tot_pZ,
							sub2.area1_tot_pZ,
							sub2.idprocpte1_tot_pZ,
							CASE
								WHEN tot_pZ.idpar IS NULL AND tot_pZbis.idpar IS NOT NULL THEN tot_pZbis.idpar
								ELSE tot_pZ.idpar
							END idpar2_tot_pZ,
							CASE
								WHEN tot_pZ.idpar IS NULL AND tot_pZbis.idpar IS NOT NULL THEN tot_pZbis.dcntpa
								ELSE tot_pZ.dcntpa
							END area2_tot_pZ,
							CASE
								WHEN tot_pZ.idpar IS NULL AND tot_pZbis.idpar IS NOT NULL THEN tot_pZbis.idprocpte
								ELSE tot_pZ.idprocpte
							END idprocpte2_tot_pZ,
							'évolution complexe avec saut de millésime' type_evol
						FROM sub2
						LEFT JOIN %I.%I tot_pZ ON sub2.idpar2 = tot_pZ.idpar---on check si on retrouve une parcelle en pZ
							AND sub2.idpar1_tot_pZ IS NULL
						LEFT JOIN %I.%I tot_pZbis ON sub2.idpar2= tot_pZbis.idpar_inverse---on regarde si on retrouve les idparfille dans les parcelles de pZ ou non
							AND sub2.idpar1_tot_pZ IS NULL)
					
					----+niveau3
					SELECT sub3.iddep22,
						sub3.idcom22,
						sub3.idfil_pX,
						sub3.idpar0,
						sub3.geom0,
						sub3.area0,
						sub3.idpar1,
						sub3.idpar2,
						sub3.idpar3,
						sub3.idpar0_tot_pZ,
						sub3.area0_tot_pZ,
						sub3.idprocpte0_tot_pZ,
						sub3.idpar1_tot_pZ,
						sub3.area1_tot_pZ,
						sub3.idprocpte1_tot_pZ,
						sub3.idpar2_tot_pZ,
						sub3.area2_tot_pZ,
						sub3.idprocpte2_tot_pZ,
						CASE
							WHEN tot_pZ.idpar IS NULL AND tot_pZbis.idpar IS NOT NULL THEN tot_pZbis.idpar
							ELSE tot_pZ.idpar
						END idpar3_tot_pZ,
						CASE
							WHEN tot_pZ.idpar IS NULL AND tot_pZbis.idpar IS NOT NULL THEN tot_pZbis.dcntpa
							ELSE tot_pZ.dcntpa
						END area3_tot_pZ,
						CASE
							WHEN tot_pZ.idpar IS NULL AND tot_pZbis.idpar IS NOT NULL THEN tot_pZbis.idprocpte
							ELSE tot_pZ.idprocpte
						END idprocpte3_tot_pZ,
						'évolution complexe avec saut de millésime' type_evol
					FROM sub3
					LEFT JOIN %I.%I tot_pZ ON sub3.idpar3 = tot_pZ.idpar---on check si on retrouve une parcelle en pZ
						AND sub3.idpar2_tot_pZ IS NULL
					LEFT JOIN %I.%I tot_pZbis ON sub3.idpar3= tot_pZbis.idpar_inverse---on regarde si on retrouve les idparfille dans les parcelles de pZ ou non
						AND sub3.idpar2_tot_pZ IS NULL
					);
				DROP TABLE anaseq.pX_complexe CASCADE;
				\$\$,
				schema_parcelles_dep_pZ, table_parcelles_dep_pZ,
				schema_parcelles_dep_pZ, table_parcelles_dep_pZ,
				schema_parcelles_dep_pZ, table_parcelles_dep_pZ,
				schema_parcelles_dep_pZ, table_parcelles_dep_pZ,
				schema_parcelles_dep_pZ, table_parcelles_dep_pZ,
				schema_parcelles_dep_pZ, table_parcelles_dep_pZ,
				schema_parcelles_dep_pZ, table_parcelles_dep_pZ,
				schema_parcelles_dep_pZ, table_parcelles_dep_pZ
				);
			COMMIT;

			---Extraction et élagage des filiations complexes qu'on retrouve dans les Fichiers Fonciers
			EXECUTE format(
				\$\$
				DROP TABLE IF EXISTS anaseq.pX_sauts_millesimes_ff CASCADE;
				CREATE TABLE anaseq.pX_sauts_millesimes_ff AS (
					SELECT iddep22,
						idcom22,
						idfil_pX,
						idpar0,
						geom0,
						area0,
						CASE
							WHEN idpar3_tot_pZ IS NOT NULL THEN idpar3_tot_pZ
							WHEN idpar2_tot_pZ IS NOT NULL THEN idpar2_tot_pZ
							WHEN idpar1_tot_pZ IS NOT NULL THEN idpar1_tot_pZ
							ELSE NULL
						END idpar_new,
						CASE
							WHEN idpar3_tot_pZ IS NOT NULL THEN idprocpte3_tot_pZ
							WHEN idpar2_tot_pZ IS NOT NULL THEN idprocpte2_tot_pZ
							WHEN idpar1_tot_pZ IS NOT NULL THEN idprocpte1_tot_pZ
							ELSE NULL
						END idprocpte_new,
						CASE
							WHEN idpar3_tot_pZ IS NOT NULL THEN area3_tot_pZ
							WHEN idpar2_tot_pZ IS NOT NULL THEN area2_tot_pZ
							WHEN idpar1_tot_pZ IS NOT NULL THEN area1_tot_pZ
							ELSE NULL
						END area_new,
						type_evol
					FROM anaseq.pX_sauts_millesimes
					WHERE idpar1_tot_pZ IS NOT NULL
						OR idpar2_tot_pZ IS NOT NULL
						OR idpar3_tot_pZ IS NOT NULL);
				\$\$);
			COMMIT;

			---Création de la table de passage temporaire pX_pZ
			EXECUTE format(
				\$\$
				DROP TABLE IF EXISTS anaseq.passage_pX_pZ CASCADE;
				CREATE TABLE anaseq.passage_pX_pZ AS (

					SELECT iddep22,
						idcom22,
						idfil_pX,
						idpar0 pX_idpar,
						geom0 pX_geom,
						area0 pX_area,
						idpar_new pZ_idpar,
						area_new pZ_area,
						idprocpte_new pZ_idprocpte,
						type_evol
					FROM anaseq.pX_sauts_millesimes_ff
					GROUP BY iddep22,
						idcom22,
						idfil_pX,
						pX_idpar,
						pX_geom,
						pX_area,
						pZ_idpar,
						pZ_area,
						pZ_idprocpte,
						type_evol);
					
				CREATE INDEX sub_pZ_idpar_idx ON anaseq.passage_pX_pZ(pZ_idpar);
				\$\$);
			COMMIT;

			---Recherche des dernières géométries parcellaires dans la table de passage pX_pZ
			EXECUTE format(
				\$\$
				---Création des colonnes et index
				ALTER TABLE anaseq.passage_pX_pZ DROP COLUMN IF EXISTS pZ_lastgeompar CASCADE;
				ALTER TABLE anaseq.passage_pX_pZ ADD COLUMN pZ_lastgeompar geometry;
				CREATE INDEX pX_pZ_passage_geom_pX_idx ON anaseq.passage_pX_pZ USING gist(pX_geom);
				CREATE INDEX pX_pZ_passage_geom_pZ_idx ON anaseq.passage_pX_pZ USING gist(pZ_lastgeompar);
				CREATE INDEX pX_pZ_passage_idpar_new_idx ON anaseq.passage_pX_pZ(pZ_idpar);
				\$\$);

			FOREACH millesime_de_reference IN ARRAY ARRAY['2021','2020','2019','2018','2017','2016','2015','2014','2013','2012','2011','2009']
			LOOP
				schema_de_reference := 'ff_'||millesime_de_reference||'_dep';
		
				IF millesime_de_reference::numeric < 2018 THEN
						table_de_reference2 := 'd'||dep||'_'||millesime_de_reference||'_pnb10_parcelle';
				ELSE
						table_de_reference2 := 'd'||dep||'_fftp_'||millesime_de_reference||'_pnb10_parcelle';
				END IF;
				table_de_reference := table_de_reference2;

				IF millesime_de_reference::numeric >= pZ::numeric-1 THEN---On ne recherche pas lastgeompar plus d'un an avant l'année considérée
			
						EXECUTE format(
							\$\$		
							UPDATE anaseq.passage_pX_pZ a
							SET pZ_lastgeompar = ST_MakeValid(b.geompar)
							FROM %I.%I b
							WHERE a.pZ_lastgeompar IS NULL
								AND b.geompar IS NOT NULL
								AND a.pZ_idpar = b.idpar
								AND b.vecteur LIKE 'V';
			
							UPDATE anaseq.passage_pX_pZ a
							SET pZ_lastgeompar = ST_MakeValid(b.geompar)
							FROM %I.%I b
							WHERE a.pZ_lastgeompar IS NULL
								AND b.geompar IS NOT NULL
								AND a.pZ_idpar = b.idpar_inverse---si changement communal
								AND b.vecteur LIKE 'V';
							\$\$,
							schema_de_reference, table_de_reference,
							schema_de_reference, table_de_reference);
			
				END IF;
			END LOOP;
			COMMIT;

			--Insert de ces filiations si lastgeompar retrouvée et valide
			EXECUTE format(
				\$\$
				INSERT INTO anaseq.%I(
					ccodep,
					iddep22,
					idcom22,
					annee,
					%I,---idfil_pX
					%I,---idpar_pX
					%I,---idpar_pY
					%I,---idpar_pZ
					%I,---idprocpte_pZ
					geom,
					type_evol
					)
				SELECT %L,
					iddep22,
					idcom22,
					%L,---année d'évolution initiale
					idfil_pX,
					pX_idpar,
					'saut',
					pZ_idpar,
					pZ_idprocpte,
					ST_Union(ST_CollectionExtract(ST_MakeValid(ST_Intersection(pX_geom,pZ_lastgeompar)),3)),
					type_evol
				FROM anaseq.passage_pX_pZ
				WHERE pX_geom IS NOT NULL
					AND pZ_lastgeompar IS NOT NULL
				GROUP BY iddep22,
					idcom22,
					idfil_pX,
					pX_idpar,
					pZ_idpar,
					pZ_idprocpte,
					type_evol;
				
				UPDATE anaseq.%I
				SET area = ST_Area(geom)
				WHERE area IS NULL
					AND annee = %L
					AND %I LIKE 'saut'
					AND geom IS NOT NULL;
				\$\$,
				table_filiation_pY,
				idfil_filiation_pX,
				pX_idpar,
				pY_idpar,
				pZ_idpar,
				pZ_idprocpte,
				dep,
				m1,
				table_filiation_pY,
				m1,
				pY_idpar);
			COMMIT;

			
			BEGIN---En cas d'erreur de géométrie...
				---Insert des non-superpositions géométriques dans la table nodfi
				EXECUTE format(
				\$\$
				DROP TABLE IF EXISTS anaseq.fil_pY_uni CASCADE;
				CREATE TABLE anaseq.fil_pY_uni AS (
					SELECT ST_Union(ST_MakeValid(ST_CollectionExtract(geom,3))) geom
					FROM anaseq.%I
					WHERE annee = %L
						AND type_evol LIKE '%%saut%%'
				);
				CREATE INDEX fil_pY_uni_geom_idx ON anaseq.fil_pY_uni USING gist(geom);
				
				DROP TABLE IF EXISTS anaseq.passuni CASCADE;
				CREATE TABLE anaseq.passuni AS (
					SELECT ST_Union(ST_MakeValid(ST_CollectionExtract(pX_geom,3))) geom
					FROM anaseq.passage_pX_pZ
					WHERE pX_geom IS NOT NULL
						AND pZ_lastgeompar IS NOT NULL
				);
				CREATE INDEX passuni_geom_idx ON anaseq.passuni USING gist(geom);
				
				DROP TABLE IF EXISTS anaseq.diff CASCADE;
				CREATE TABLE anaseq.diff AS (
					SELECT ST_MakeValid(ST_CollectionExtract(ST_Difference(passuni.geom, fil_pY_uni.geom),3)) geom
					FROM anaseq.fil_pY_uni, anaseq.passuni	
				);
				
				CREATE INDEX diff_geom_idx ON anaseq.diff USING gist(geom);
				\$\$,
				table_filiation_pY,
				m1
				);

			EXCEPTION----En cas d'erreur de différence spatiale, on reproduit ST_MemUnion sur la table anaseq.fil_pY_uni
				WHEN OTHERS THEN
					
					EXECUTE format(
					\$\$
					DROP TABLE IF EXISTS anaseq.fil_pY_uni CASCADE;
					CREATE TABLE anaseq.fil_pY_uni AS (
						SELECT ST_MemUnion(ST_MakeValid(ST_CollectionExtract(geom,3))) geom
						FROM anaseq.%I
						WHERE annee = %L
							AND type_evol LIKE '%%saut%%'
					);
					CREATE INDEX fil_pY_uni_geom_idx ON anaseq.fil_pY_uni USING gist(geom);
					
					DROP TABLE IF EXISTS anaseq.passuni CASCADE;
					CREATE TABLE anaseq.passuni AS (
						SELECT ST_Union(ST_MakeValid(ST_CollectionExtract(pX_geom,3))) geom
						FROM anaseq.passage_pX_pZ
						WHERE pX_geom IS NOT NULL
							AND pZ_lastgeompar IS NOT NULL
					);
					CREATE INDEX passuni_geom_idx ON anaseq.passuni USING gist(geom);
					
					CREATE TABLE anaseq.diff AS (
						SELECT ST_MakeValid(ST_CollectionExtract(ST_Difference(passuni.geom, fil_pY_uni.geom),3)) geom
						FROM anaseq.fil_pY_uni, anaseq.passuni	
					);
					CREATE INDEX diff_geom_idx ON anaseq.diff USING gist(geom);
					\$\$,
					table_filiation_pY,
					m1);
				
			END;	

			EXECUTE format(
				\$\$	
				INSERT INTO anaseq.reste_pX_nodfi(
					iddep22,
					idcom22,
					idfil_pX,
					pX_idpar,
					pX_geom)
				SELECT iddep22,
					idcom22,
					idfil_pX,
					pX_idpar,
					ST_Union(ST_Difference(pX_geom,pZ_lastgeompar))
				FROM anaseq.passage_pX_pZ---table de passage préétablie
				WHERE pX_geom IS NOT NULL
					AND pZ_lastgeompar IS NOT NULL
				GROUP BY iddep22,
					idcom22,
					idfil_pX,
					pX_idpar;
				
				UPDATE anaseq.reste_pX_nodfi
				SET pX_area = ST_Area(pX_geom)
				WHERE pX_area IS NULL
					AND pX_geom IS NOT NULL;

				DROP TABLE anaseq.fil_pY_uni CASCADE;
				DROP TABLE anaseq.passuni CASCADE;
				DROP TABLE anaseq.diff CASCADE;
				\$\$);
			COMMIT;

			---On compte pour chaque parcelle en pX la surface non-retrouvée géométriquement
			---> pour distibuter équitablement la surface restante entre les parcelles filles/mères
			EXECUTE format(
				\$\$
				ALTER TABLE anaseq.passage_pX_pZ ADD COLUMN surf_geom_manquante numeric;

				WITH sub AS(
					SELECT %I,
						SUM(area) area_retrouvee
					FROM anaseq.%I
					WHERE annee = %L
						AND %I LIKE 'saut'
					GROUP BY %I
				)
				UPDATE anaseq.passage_pX_pZ a
				SET surf_geom_manquante = COALESCE(COALESCE(a.pX_area,0)-COALESCE(sub.area_retrouvee,0),0)
				FROM sub
				WHERE a.pX_idpar = sub.%I;

				UPDATE anaseq.passage_pX_pZ
				SET surf_geom_manquante = pX_area
				WHERE surf_geom_manquante IS NULL
				\$\$,
				pX_idpar,
				table_filiation_pY,
				m1,
				pY_idpar,
				pX_idpar,
				pX_idpar);
			COMMIT;

			---Table des parcelles avec lacune géométrique
			EXECUTE format(
				\$\$
				DROP TABLE IF EXISTS anaseq.passage_pX_pZ_lacunesgeom CASCADE;
				CREATE TABLE anaseq.passage_pX_pZ_lacunesgeom AS (
					SELECT * FROM anaseq.passage_pX_pZ
					WHERE pX_geom IS NULL
						OR pZ_lastgeompar IS NULL);
				\$\$);
			COMMIT;

			--Calcul du nombre de filles et de mères
			EXECUTE format(
				\$\$
				ALTER TABLE anaseq.passage_pX_pZ_lacunesgeom ADD COLUMN nb_meres int;
				ALTER TABLE anaseq.passage_pX_pZ_lacunesgeom ADD COLUMN nb_filles int;

				WITH sub AS (
					SELECT pX_idpar, COUNT(DISTINCT pZ_idpar) nb_filles
					FROM anaseq.passage_pX_pZ_lacunesgeom
					GROUP BY pX_idpar)
				UPDATE anaseq.passage_pX_pZ_lacunesgeom a
				SET nb_filles = sub.nb_filles
				FROM sub
				WHERE a.pX_idpar = sub.pX_idpar;

				WITH sub AS (
					SELECT pZ_idpar, COUNT(DISTINCT pX_idpar) nb_meres
					FROM anaseq.passage_pX_pZ_lacunesgeom
					GROUP BY pZ_idpar			
				)
				UPDATE anaseq.passage_pX_pZ_lacunesgeom a
				SET nb_meres = sub.nb_meres
				FROM sub
				WHERE a.pZ_idpar = sub.pZ_idpar;
				\$\$);
			COMMIT;

			---Insert de ces filiations si lastgeompar non-retrouvée
			EXECUTE format(
				\$\$
				INSERT INTO anaseq.%I(
					ccodep,
					iddep22,
					idcom22,
					annee,
					%I,---idfil_pX
					%I,---idpar_pX
					%I,---idpar_pY
					%I,---idpar_pZ
					%I,---idprocpte_pZ
					geom,
					area,
					type_evol
					)
				SELECT %L,
					iddep22,
					idcom22,
					%L,---année d'évolution initiale
					idfil_pX,
					pX_idpar,
					'saut',
					pZ_idpar,
					pZ_idprocpte,
					CASE
						WHEN type_evol LIKE 'fusion'
							OR type_evol LIKE 'transfert'
							OR (nb_meres >= 1 AND nb_filles = 1)
								THEN pX_geom
						ELSE NULL::geometry
					END,
					CASE
						WHEN type_evol LIKE 'fusion'
							OR type_evol LIKE 'transfert'
							OR (nb_meres >= 1 AND nb_filles = 1)
								THEN pX_area
						ELSE surf_geom_manquante/(nb_meres+nb_filles-1)---divise la surface par le nombre de portions
					END,
					CONCAT(type_evol, ' (avec géométrie invalide)')
				FROM anaseq.passage_pX_pZ_lacunesgeom sub
				WHERE pX_geom IS NULL
					OR pZ_lastgeompar IS NULL
				GROUP BY iddep22,
					idcom22,
					idfil_pX,
					pX_idpar,
					pZ_idpar,
					pZ_idprocpte,
					nb_meres,
					nb_filles,
					pX_geom,
					pX_area,
					surf_geom_manquante,
					type_evol;
				\$\$,
				table_filiation_pY,
				idfil_filiation_pX,
				pX_idpar,
				pY_idpar,
				pZ_idpar,
				pZ_idprocpte,
				dep,
				m1);
			COMMIT;

			---Suppression des parcelles retrouvées
			---et insert dans la table des non-DFI des parcelles non-retrouvées
			EXECUTE format(
				\$\$
				DELETE FROM anaseq.pX_sauts_millesimes saut
				USING anaseq.pX_sauts_millesimes_ff ff
				WHERE saut.idpar0 = ff.idpar0
					AND ff.idpar_new IS NOT NULL;
				
				DROP TABLE anaseq.pX_sauts_millesimes_ff CASCADE;
				DROP TABLE anaseq.passage_pX_pZ CASCADE;
				DROP TABLE anaseq.passage_pX_pZ_lacunesgeom CASCADE;
				
				INSERT INTO anaseq.reste_pX_nodfi
					SELECT iddep22,
						idcom22,
						idfil_pX,
						idpar0,
						geom0,
						area0
					FROM anaseq.pX_sauts_millesimes;
				DROP TABLE anaseq.pX_sauts_millesimes CASCADE;
				\$\$);
			COMMIT;
		END IF;
		

		-------------------------------------------
		---Approche géométrique pour les parcelles sans DFI ou les parcelles en NC
		-------------------------------------------
		RAISE NOTICE '-------------BRANCHE 2 (APPROCHE GEOMETRIQUE)-------------';
		
		FOR idcom22 IN EXECUTE 'SELECT LEFT(idcom22,3) FROM anaseq.reste_pX_nodfi GROUP BY LEFT(idcom22,3) ORDER BY LEFT(idcom22,3)'---On boucle sur des groupes de communes pour alléger la mémoire vive
		LOOP
			
			RAISE NOTICE 'Communes %',idcom22;
		
			---Création des géométries des parcelles à filier
			EXECUTE format(
				\$\$
				DROP TABLE IF EXISTS anaseq.reste_pX_nodfi_com CASCADE;
				CREATE TABLE anaseq.reste_pX_nodfi_com AS (
					SELECT * FROM anaseq.reste_pX_nodfi WHERE LEFT(idcom22,3) LIKE %L);	
					
				ALTER TABLE anaseq.reste_pX_nodfi_com DROP COLUMN IF EXISTS pX_lastgeompoint CASCADE;
				ALTER TABLE anaseq.reste_pX_nodfi_com DROP COLUMN IF EXISTS pX_lastgeompoint_buffer200 CASCADE;
				ALTER TABLE anaseq.reste_pX_nodfi_com DROP COLUMN IF EXISTS pX_lastgeompar_bufferneg2 CASCADE;
				ALTER TABLE anaseq.reste_pX_nodfi_com ADD COLUMN pX_lastgeompoint geometry;
				ALTER TABLE anaseq.reste_pX_nodfi_com ADD COLUMN pX_lastgeompoint_buffer200 geometry;
				ALTER TABLE anaseq.reste_pX_nodfi_com ADD COLUMN pX_lastgeompar_bufferneg2 geometry;
				CREATE INDEX IF NOT EXISTS reste_pX_nodfi_pX_lastgeompoint_idx ON anaseq.reste_pX_nodfi_com USING gist(pX_lastgeompoint);
				CREATE INDEX IF NOT EXISTS reste_pX_nodfi_pX_lastgeompoint_buffer200_idx ON anaseq.reste_pX_nodfi_com USING gist(pX_lastgeompoint_buffer200);
				CREATE INDEX IF NOT EXISTS reste_pX_nodfi_pX_lastgeompar_bufferneg2_idx ON anaseq.reste_pX_nodfi_com USING gist(pX_lastgeompar_bufferneg2);
				CREATE INDEX IF NOT EXISTS reste_pX_nodfi_pX_idpar_idx ON anaseq.reste_pX_nodfi_com(pX_idpar);
				\$\$,
				idcom22);
			COMMIT;

			---Recherche du centroïde des parcelles ou portions de parcelles	
			EXECUTE format(
				\$\$
				DELETE FROM anaseq.reste_pX_nodfi_com
				WHERE pX_geom IS NOT NULL
					AND pX_area <= 0;
				
				UPDATE anaseq.reste_pX_nodfi_com a
				SET pX_lastgeompoint = ST_PointOnSurface(pX_geom);
		
				UPDATE anaseq.reste_pX_nodfi_com
				SET pX_lastgeompoint_buffer200 = ST_Buffer(pX_lastgeompoint,200)---pour rechercher les parcelles qui l'intersectent
				WHERE pX_lastgeompoint IS NOT NULL;
				
				UPDATE anaseq.reste_pX_nodfi_com
				SET pX_lastgeompar_bufferneg2 = ST_Buffer(pX_geom,-2)---pour éviter les superpositions accidentelles
				WHERE pX_geom IS NOT NULL;
				\$\$);
			COMMIT;

			---Sélection de toutes les sections de parcelles en pY
			---qui intersectent le buffer de 200m (ce qui permet de prendre les parcelles avec lacune géométrique pour essayer de la corriger)
			EXECUTE format(
				\$\$
				DROP TABLE IF EXISTS anaseq.par_pY CASCADE;
				CREATE TABLE anaseq.par_pY AS (
					WITH liste_sections AS (
						SELECT tot_pY.ccodep,
							tot_pY.idcom,
							tot_pY.idsec
						FROM anaseq.reste_pX_nodfi_com nodfi
						JOIN %I.%I tot_pY
							ON ST_Intersects(nodfi.pX_lastgeompoint_buffer200, tot_pY.geompar)
						GROUP BY tot_pY.ccodep,
							tot_pY.idcom,
							tot_pY.idsec)
					
					SELECT tot_pY.ccodep,
						tot_pY.idcom,
						tot_pY.idpar,
						tot_pY.idprocpte
					FROM %I.%I tot_pY
					JOIN liste_sections sec ON tot_pY.idsec = sec.idsec
				);
				\$\$,
				schema_parcelles_dep_pY, table_parcelles_dep_pY,
				schema_parcelles_dep_pY, table_parcelles_dep_pY);
			COMMIT;

			---Recherche de la dernière géométrie en par_pY
			EXECUTE format(
				\$\$
				ALTER TABLE anaseq.par_pY DROP COLUMN IF EXISTS lastgeompar CASCADE;
				ALTER TABLE anaseq.par_pY ADD COLUMN lastgeompar geometry;
				CREATE INDEX par_pY_lastgeompar_idx ON anaseq.par_pY USING gist(lastgeompar);
				CREATE INDEX par_pY_idpar ON anaseq.par_pY(idpar);
				\$\$);
			COMMIT;
		
			FOREACH millesime_de_reference IN ARRAY ARRAY['2021','2020','2019','2018','2017','2016','2015','2014','2013','2012','2011','2009']
			LOOP
				schema_de_reference := 'ff_'||millesime_de_reference||'_dep';
		
				IF millesime_de_reference::numeric < 2018 THEN
						table_de_reference2 := 'd'||dep||'_'||millesime_de_reference||'_pnb10_parcelle';
				ELSE
						table_de_reference2 := 'd'||dep||'_fftp_'||millesime_de_reference||'_pnb10_parcelle';
				END IF;
				table_de_reference := table_de_reference2;

				IF millesime_de_reference::numeric >= pY::numeric-1 THEN---On ne recherche pas lastgeompar plus d'un an avant l'année considérée
							
						EXECUTE format(
							\$\$
							UPDATE anaseq.par_pY a
							SET lastgeompar = ST_MakeValid(b.geompar)
							FROM %I.%I b
							WHERE a.lastgeompar IS NULL
								AND b.geompar IS NOT NULL
								AND a.idpar = b.idpar
								AND b.vecteur LIKE 'V';
			
							UPDATE anaseq.par_pY a
							SET lastgeompar = ST_MakeValid(b.geompar)
							FROM %I.%I b
							WHERE a.lastgeompar IS NULL
								AND b.geompar IS NOT NULL
								AND a.idpar = b.idpar_inverse
								AND b.vecteur LIKE 'V';
							\$\$,
							schema_de_reference, table_de_reference,
							schema_de_reference, table_de_reference);
				END IF;
				COMMIT;
			END LOOP;
			COMMIT;


			--- Insert des portions de parcelles retrouvées
			EXECUTE format(
				\$\$
				WITH sub AS (
					SELECT nodfi.iddep22,
						nodfi.idcom22,---commune concernée
						nodfi.idfil_pX,
						ST_Union(ST_CollectionExtract(ST_MakeValid(ST_Intersection(nodfi.pX_geom,par_pY.lastgeompar)),3)) geom,
						nodfi.pX_idpar,---parcelle en pX
						par_pY.idpar pY_idpar,---parcelle en pY
						par_pY.idprocpte pY_idprocpte
					FROM anaseq.reste_pX_nodfi_com nodfi
					JOIN anaseq.par_pY par_pY ON ST_Intersects(nodfi.pX_lastgeompar_bufferneg2,par_pY.lastgeompar)
					GROUP BY nodfi.iddep22,
						nodfi.idcom22,---commune concernée
						nodfi.idfil_pX,
						nodfi.pX_idpar,
						par_pY.idpar,
						par_pY.idprocpte)
							
				INSERT INTO anaseq.%I(
					ccodep,
					iddep22,
					idcom22,
					annee,
					%I,---idfil_pX
					%I,---idpar_pX
					%I,---idpar_pY
					%I,---idprocpte_pY
					%I,---idpar_pZ
					geom,
					type_evol
					)
				SELECT %L,
					iddep22,
					idcom22,
					%L,---annee d'évolution initiale
					idfil_pX,
					pX_idpar,
					pY_idpar,
					pY_idprocpte,
					'SO',---sans objet en pZ
					geom,
					'filiation géométrique'
				FROM sub
				GROUP BY iddep22,
					idcom22,---commune concernée
					idfil_pX,
					pX_idpar,
					pY_idpar,
					pY_idprocpte,
					geom
				ON CONFLICT DO NOTHING;---évite les duplicats en cas d'erreur de géométrie
				
				UPDATE anaseq.%I SET area = ST_Area(geom)
				WHERE LEFT(idcom22,3) LIKE %L
					AND type_evol LIKE 'filiation géométrique';
				\$\$,
				table_filiation_pY,
				idfil_filiation_pX,
				pX_idpar,
				pY_idpar,
				pY_idprocpte,
				pZ_idpar,
				dep,
				m1,
				table_filiation_pY,
				idcom22);
			COMMIT;

							
			BEGIN---En cas d'erreur de géométrie
			---Création de la table des géométries parcellaires non retrouvées à ce stade
				EXECUTE format(
					\$\$
					DROP TABLE IF EXISTS anaseq.pX_nodfi_uni CASCADE;
					CREATE TABLE anaseq.pX_nodfi_uni AS (
						SELECT ST_Union(ST_MakeValid(ST_CollectionExtract(pX_geom,3))) geom
						FROM anaseq.reste_pX_nodfi_com);
					CREATE INDEX nodfi_uni_geom_idx ON anaseq.pX_nodfi_uni USING gist(geom);
					
					DROP TABLE IF EXISTS anaseq.pX_dfi_retrouve_uni CASCADE;
					CREATE TABLE anaseq.pX_dfi_retrouve_uni AS (
						SELECT ST_Union(ST_MakeValid(ST_CollectionExtract(geom,3))) geom
						FROM anaseq.%I
						WHERE annee = %L
							AND LEFT(idcom22,3) LIKE %L
							AND type_evol LIKE 'filiation géométrique');
					CREATE INDEX dfi_retrouve_uni_geom_idx ON anaseq.pX_dfi_retrouve_uni USING gist(geom);
					
					DROP TABLE IF EXISTS anaseq.pX_diff CASCADE;
					CREATE TABLE anaseq.pX_diff AS (
					SELECT ST_Union(ST_CollectionExtract(ST_MakeValid(ST_Difference(nodfi.geom,dfi_retrouve_uni.geom)),3)) geom
					FROM anaseq.pX_nodfi_uni nodfi, anaseq.pX_dfi_retrouve_uni dfi_retrouve_uni);
					CREATE INDEX diff_geom_idx ON anaseq.pX_diff USING gist(geom);
					\$\$,
					table_filiation_pY,
					m1,
					idcom22
				);

			EXCEPTION----En cas d'erreur de différence spatiale, on refait avec ST_MemUnion sur la table anaseq.fil_pY_uni
				WHEN OTHERS THEN
					
					EXECUTE format(
					\$\$
					DROP TABLE IF EXISTS anaseq.pX_nodfi_uni CASCADE;
					CREATE TABLE anaseq.pX_nodfi_uni AS (
						SELECT ST_MemUnion(ST_MakeValid(ST_CollectionExtract(pX_geom,3))) geom
						FROM anaseq.reste_pX_nodfi_com);
					CREATE INDEX nodfi_uni_geom_idx ON anaseq.pX_nodfi_uni USING gist(geom);
					
					DROP TABLE IF EXISTS anaseq.pX_dfi_retrouve_uni CASCADE;
					CREATE TABLE anaseq.pX_dfi_retrouve_uni AS (
						SELECT ST_MemUnion(ST_MakeValid(ST_CollectionExtract(geom,3))) geom
						FROM anaseq.%I
						WHERE annee = %L
							AND LEFT(idcom22,3) LIKE %L
							AND type_evol LIKE 'filiation géométrique');
					CREATE INDEX dfi_retrouve_uni_geom_idx ON anaseq.pX_dfi_retrouve_uni USING gist(geom);
					
					DROP TABLE IF EXISTS anaseq.pX_diff CASCADE;
					CREATE TABLE anaseq.pX_diff AS (
						SELECT ST_Union(ST_CollectionExtract(ST_MakeValid(ST_Difference(nodfi.geom,dfi_retrouve_uni.geom)),3)) geom
						FROM anaseq.pX_nodfi_uni nodfi, anaseq.pX_dfi_retrouve_uni dfi_retrouve_uni);
					CREATE INDEX diff_geom_idx ON anaseq.pX_diff USING gist(geom);
					\$\$,
					table_filiation_pY,
					m1,
					idcom22
				);
				
			END;	
			
			EXECUTE format(
				\$\$			
				DROP TABLE IF EXISTS anaseq.pX_nodfi_etape2 CASCADE;
				CREATE TABLE anaseq.pX_nodfi_etape2 AS (
					SELECT nodfi.iddep22,
						nodfi.idcom22,
						nodfi.idfil_pX,
						nodfi.pX_idpar,
						ST_Union(ST_CollectionExtract(ST_MakeValid(ST_Intersection(nodfi.pX_geom,diff.geom)),3)) geom
					FROM anaseq.reste_pX_nodfi_com nodfi
					JOIN anaseq.pX_diff diff ON ST_Intersects(nodfi.pX_lastgeompar_bufferneg2,diff.geom)
					GROUP BY nodfi.iddep22,
						nodfi.idcom22,
						nodfi.idfil_pX,
						nodfi.pX_idpar
				);
				
				ALTER TABLE anaseq.pX_nodfi_etape2 ADD COLUMN geom_bufferneg2 geometry;
				UPDATE anaseq.pX_nodfi_etape2 SET geom_bufferneg2 = ST_Buffer(geom,-2);
				CREATE INDEX nodfi_etape2_geom_idx ON anaseq.pX_nodfi_etape2 USING gist(geom);
				CREATE INDEX nodfi_etape2_geom_bufferneg2_idx ON anaseq.pX_nodfi_etape2 USING gist(geom_bufferneg2);
				
				DROP TABLE anaseq.pX_nodfi_uni CASCADE;
				DROP TABLE anaseq.pX_dfi_retrouve_uni CASCADE;
				DROP TABLE anaseq.pX_diff CASCADE;
				DROP TABLE anaseq.reste_pX_nodfi_com CASCADE;
				DROP TABLE anaseq.par_pY CASCADE;
				\$\$
			);
			COMMIT;

			---Insert des maintiens en NC (les NC en pX non retrouvés en parcelle en pY)
			EXECUTE format(
				\$\$
				INSERT INTO anaseq.%I(
					ccodep,
					iddep22,
					idcom22,
					annee,
					%I,---idfil_pX
					%I,---idpar_pX
					%I,---idpar_pY
					%I,---idprocpte_pY
					%I,---idpar_pZ
					geom,
					area,
					type_evol
					)
				SELECT %L,
					iddep22,
					idcom22,
					%L,---annee d'évolution initiale
					idfil_pX,
					pX_idpar,
					'NC',---pY_idpar
					'NC',---pY_idprocpte
					'SO',---sans objet en pZ
					geom,
					ST_Area(geom),
					'maintien en NC'
				FROM anaseq.pX_nodfi_etape2
				WHERE pX_idpar LIKE 'NC'
				GROUP BY iddep22,
					idcom22,---commune concernée
					idfil_pX,
					pX_idpar,
					geom
				ON CONFLICT DO NOTHING;---évite les duplicats en cas d'erreur de géométrie

				DELETE FROM anaseq.pX_nodfi_etape2 WHERE pX_idpar LIKE 'NC';
				\$\$,
				table_filiation_pY,
				idfil_filiation_pX,
				pX_idpar,
				pY_idpar,
				pY_idprocpte,
				pZ_idpar,
				dep,
				m1);
			COMMIT;


			IF X < 4 THEN---pas de saut de millésime à partir de 2021
				------------------------------------------
				---Approche géométrique avec saut de millésime pour les parcelles restantes
				-------------------------------------------
				RAISE NOTICE 'APPROCHE GEOMETRIQUE POUR SAUTS DE MILLESIME';

				---Création des centroïdes et buffer sur les parcelles restantes
				EXECUTE format(
					\$\$
					ALTER TABLE anaseq.pX_nodfi_etape2 DROP COLUMN IF EXISTS lastgeompoint;
					ALTER TABLE anaseq.pX_nodfi_etape2 DROP COLUMN IF EXISTS lastgeompoint_buffer200;
					ALTER TABLE anaseq.pX_nodfi_etape2 ADD COLUMN lastgeompoint geometry;
					ALTER TABLE anaseq.pX_nodfi_etape2 ADD COLUMN lastgeompoint_buffer200 geometry;
					CREATE INDEX pX_nodfi_etape2_lastgeompoint_buffer200_idx ON anaseq.pX_nodfi_etape2 USING gist(lastgeompoint_buffer200);
					
					UPDATE anaseq.pX_nodfi_etape2
					SET lastgeompoint = ST_PointOnSurface(geom);
					UPDATE anaseq.pX_nodfi_etape2
					SET lastgeompoint_buffer200 = ST_Buffer(lastgeompoint,200);
					\$\$);
				COMMIT;

				---Sélection de toutes les sections de parcelles en pZ
				---qui intersectent le buffer de 200m (ce qui permet de prendre les parcelles avec lacune géométrique pour essayer de la corriger)
				EXECUTE format(
					\$\$
					DROP TABLE IF EXISTS anaseq.par_pZ CASCADE;
					CREATE TABLE anaseq.par_pZ AS (
						WITH liste_sections AS (
							SELECT tot_pZ.ccodep,
								tot_pZ.idcom,
								tot_pZ.idsec
							FROM anaseq.pX_nodfi_etape2 nodfi
							JOIN %I.%I tot_pZ
								ON ST_Intersects(nodfi.lastgeompoint_buffer200, tot_pZ.geompar)
							GROUP BY tot_pZ.ccodep,
								tot_pZ.idcom,
								tot_pZ.idsec)
						
						SELECT tot_pZ.ccodep,
							tot_pZ.idcom,
							tot_pZ.idpar,
							tot_pZ.idprocpte
						FROM %I.%I tot_pZ
						JOIN liste_sections sec ON tot_pZ.idsec = sec.idsec
					);
					\$\$,
					schema_parcelles_dep_pZ, table_parcelles_dep_pZ,
					schema_parcelles_dep_pZ, table_parcelles_dep_pZ);
				COMMIT;

				---Recherche de la dernière géométrie en par_pZ
				EXECUTE format(
					\$\$
					ALTER TABLE anaseq.par_pZ DROP COLUMN IF EXISTS lastgeompar CASCADE;
					ALTER TABLE anaseq.par_pZ ADD COLUMN lastgeompar geometry;
					CREATE INDEX par_pZ_lastgeompar_idx ON anaseq.par_pZ USING gist(lastgeompar);
					CREATE INDEX par_pZ_idpar ON anaseq.par_pZ(idpar);
					\$\$);
				COMMIT;
			
				FOREACH millesime_de_reference IN ARRAY ARRAY['2021','2020','2019','2018','2017','2016','2015','2014','2013','2012','2011','2009']
				LOOP
					schema_de_reference := 'ff_'||millesime_de_reference||'_dep';
			
					IF millesime_de_reference::numeric < 2018 THEN
							table_de_reference2 := 'd'||dep||'_'||millesime_de_reference||'_pnb10_parcelle';
					ELSE
							table_de_reference2 := 'd'||dep||'_fftp_'||millesime_de_reference||'_pnb10_parcelle';
					END IF;
					table_de_reference := table_de_reference2;

					IF millesime_de_reference::numeric >= pZ::numeric-1 THEN---On ne recherche pas lastgeompar plus d'un an avant l'année considérée
						
							EXECUTE format(
								\$\$
								UPDATE anaseq.par_pZ a
								SET lastgeompar = ST_MakeValid(b.geompar)
								FROM %I.%I b
								WHERE a.lastgeompar IS NULL
									AND b.geompar IS NOT NULL
									AND a.idpar = b.idpar
									AND b.vecteur LIKE 'V';
				
								UPDATE anaseq.par_pZ a
								SET lastgeompar = ST_MakeValid(b.geompar)
								FROM %I.%I b
								WHERE a.lastgeompar IS NULL
									AND b.geompar IS NOT NULL
									AND a.idpar = b.idpar_inverse
									AND b.vecteur LIKE 'V';
								\$\$,
								schema_de_reference, table_de_reference,
								schema_de_reference, table_de_reference);
					END IF;
					COMMIT;
								
				END LOOP;
				COMMIT;


				--- Insert des portions de parcelles retrouvées
				EXECUTE format(
					\$\$
					WITH sub AS (
						SELECT nodfi.iddep22,
							nodfi.idcom22,---commune concernée
							nodfi.idfil_pX,
							ST_Union(ST_CollectionExtract(ST_MakeValid(ST_Intersection(nodfi.geom,par_pZ.lastgeompar)),3)) geom,
							nodfi.pX_idpar,---parcelle en pX
							par_pZ.idpar pZ_idpar,---parcelle en pZ
							par_pZ.idprocpte pZ_idprocpte
						FROM anaseq.pX_nodfi_etape2 nodfi
						JOIN anaseq.par_pZ par_pZ ON ST_Intersects(nodfi.geom_bufferneg2,par_pZ.lastgeompar)
						GROUP BY nodfi.iddep22,
							nodfi.idcom22,---commune concernée
							nodfi.idfil_pX,
							nodfi.pX_idpar,
							par_pZ.idpar,
							par_pZ.idprocpte)
								
					INSERT INTO anaseq.%I(
						ccodep,
						iddep22,
						idcom22,
						annee,
						%I,---idfil_pX
						%I,---idpar_pX
						%I,---idpar_pY
						%I,---idpar_pZ
						%I,---idprocpte_pZ
						geom,
						type_evol
						)
					SELECT %L,
						iddep22,
						idcom22,
						%L,---annee d'évolution initiale
						idfil_pX,
						pX_idpar,
						'saut',---pY_idpar
						pZ_idpar,
						pZ_idprocpte,
						geom,
						'filiation géométrique avec saut de millésime'
					FROM sub
					GROUP BY iddep22,
						idcom22,---commune concernée
						idfil_pX,
						pX_idpar,
						pZ_idpar,
						pZ_idprocpte,
						geom
					ON CONFLICT DO NOTHING;---évite les duplicats en cas d'erreur de géométrie
					
					UPDATE anaseq.%I SET area = ST_Area(geom) WHERE type_evol LIKE 'filiation géométrique avec saut de millésime';
					\$\$,
					table_filiation_pY,
					idfil_filiation_pX,
					pX_idpar,
					pY_idpar,
					pZ_idpar,
					pZ_idprocpte,
					dep,
					m1,
					table_filiation_pY);
				COMMIT;
			END IF;


			------------------------------------------
			---Passage du reste en non-cadastré
			-------------------------------------------
			RAISE NOTICE 'PASSAGE DU RESTE EN NON-CADASTRE';

			BEGIN---en cas d'erreur de géométrie...
			---Création de la table des géométries parcellaires non retrouvées
				EXECUTE format(
					\$\$
					DROP TABLE IF EXISTS anaseq.pX_nodfi_uni CASCADE;
					CREATE TABLE anaseq.pX_nodfi_uni AS (
						SELECT ST_Union(ST_MakeValid(ST_CollectionExtract(geom,3))) geom
						FROM anaseq.pX_nodfi_etape2);
					CREATE INDEX nodfi_uni_geom_idx ON anaseq.pX_nodfi_uni USING gist(geom);
					
					DROP TABLE IF EXISTS anaseq.pX_dfi_retrouve_uni CASCADE;
					CREATE TABLE anaseq.pX_dfi_retrouve_uni AS (
						SELECT ST_Union(ST_MakeValid(ST_CollectionExtract(geom,3))) geom
						FROM anaseq.%I
						WHERE annee = %L
							AND LEFT(idcom22,3) LIKE %L
							AND type_evol LIKE 'filiation géométrique avec saut de millésime');
					CREATE INDEX dfi_retrouve_uni_geom_idx ON anaseq.pX_dfi_retrouve_uni USING gist(geom);
					
					DROP TABLE IF EXISTS anaseq.pX_diff CASCADE;
					CREATE TABLE anaseq.pX_diff AS (
						SELECT ST_Union(ST_CollectionExtract(ST_MakeValid(ST_Difference(nodfi.geom,dfi_retrouve_uni.geom)),3)) geom
						FROM anaseq.pX_nodfi_uni nodfi, anaseq.pX_dfi_retrouve_uni dfi_retrouve_uni);
					
					CREATE INDEX diff_geom_idx ON anaseq.pX_diff USING gist(geom);
					\$\$,
					table_filiation_pY,
					m1,
					idcom22
				);

			EXCEPTION----En cas d'erreur de différence spatiale, on refait avec ST_MemUnion
				WHEN OTHERS THEN
					
					EXECUTE format(
						\$\$
						DROP TABLE IF EXISTS anaseq.pX_nodfi_uni CASCADE;
						CREATE TABLE anaseq.pX_nodfi_uni AS (
							SELECT ST_MemUnion(ST_MakeValid(ST_CollectionExtract(geom,3))) geom
							FROM anaseq.pX_nodfi_etape2);
						CREATE INDEX nodfi_uni_geom_idx ON anaseq.pX_nodfi_uni USING gist(geom);
						
						DROP TABLE IF EXISTS anaseq.pX_dfi_retrouve_uni CASCADE;
						CREATE TABLE anaseq.pX_dfi_retrouve_uni AS (
							SELECT ST_MemUnion(ST_MakeValid(ST_CollectionExtract(geom,3))) geom
							FROM anaseq.%I
							WHERE annee = %L
								AND LEFT(idcom22,3) LIKE %L
								AND type_evol LIKE 'filiation géométrique avec saut de millésime');
						CREATE INDEX dfi_retrouve_uni_geom_idx ON anaseq.pX_dfi_retrouve_uni USING gist(geom);
						
						DROP TABLE IF EXISTS anaseq.pX_diff CASCADE;
						CREATE TABLE anaseq.pX_diff AS (
							SELECT ST_Union(ST_CollectionExtract(ST_MakeValid(ST_Difference(nodfi.geom,dfi_retrouve_uni.geom)),3)) geom
							FROM anaseq.pX_nodfi_uni nodfi, anaseq.pX_dfi_retrouve_uni dfi_retrouve_uni);
						CREATE INDEX diff_geom_idx ON anaseq.pX_diff USING gist(geom);
						\$\$,
						table_filiation_pY,
						m1,
						idcom22
					);

			END;	
			
			EXECUTE format(
				\$\$
				DROP TABLE IF EXISTS anaseq.pX_non_cadastre CASCADE;
				CREATE TABLE anaseq.pX_non_cadastre AS (
					SELECT nodfi.iddep22,
						nodfi.idcom22,
						nodfi.idfil_pX,
						nodfi.pX_idpar,
						ST_Union(ST_CollectionExtract(ST_MakeValid(ST_Intersection(nodfi.geom,diff.geom)),3)) geom
					FROM anaseq.pX_nodfi_etape2 nodfi
					JOIN anaseq.pX_diff diff ON ST_Intersects(nodfi.geom_bufferneg2,diff.geom)
					GROUP BY nodfi.iddep22,
						nodfi.idcom22,
						nodfi.idfil_pX,
						nodfi.pX_idpar
				);
				ALTER TABLE anaseq.pX_non_cadastre ADD COLUMN area numeric;
				UPDATE anaseq.pX_non_cadastre SET area = ST_Area(geom);
				\$\$
				);
			COMMIT;

			---Insert dans la table de filiation
			EXECUTE format(
				\$\$
				INSERT INTO anaseq.%I(
					ccodep,
					iddep22,
					idcom22,
					annee,
					%I,---idfil_pX
					%I,---idpar_pX
					%I,---idpar_pY
					%I,---idprocpte_pY
					%I,---idpar_pZ
					geom,
					area,
					type_evol
					)
				SELECT %L,
					iddep22,
					idcom22,
					%L,---annee d'évolution initiale
					idfil_pX,
					pX_idpar,
					'NC',---pY_idpar
					'NC',---pY_idprocpte
					'SO',---pZ_idpar
					geom,
					area,
					'passage en NC (absence des FF et DFI)'
				FROM anaseq.pX_non_cadastre
				GROUP BY iddep22,
					idcom22,---commune concernée
					idfil_pX,
					pX_idpar,
					geom,
					area
				ON CONFLICT DO NOTHING;---évite les duplicats en cas d'erreur
				\$\$,
				table_filiation_pY,
				idfil_filiation_pX,
				pX_idpar,
				pY_idpar,
				pY_idprocpte,
				pZ_idpar,
				dep,
				m1
			);
			COMMIT;
		END LOOP;
		
		DROP TABLE anaseq.reste_pX_nodfi CASCADE;


		------------------------------------------
		---Suppression des surfaces assimilées à de bruit
		-------------------------------------------
		RAISE NOTICE 'SUPPRESSION DU BRUIT';

		EXECUTE format(
			\$\$
			DELETE FROM anaseq.%I
			WHERE area <= 10
			\$\$,
			table_filiation_pY);
		COMMIT;


		---Suppression des tables temporaires
		EXECUTE format(
			\$\$
			DROP TABLE IF EXISTS anaseq.pX_nodfi_uni CASCADE;
			DROP TABLE IF EXISTS anaseq.pX_dfi_retrouve_uni CASCADE;
			DROP TABLE IF EXISTS anaseq.pX_diff CASCADE;
			DROP TABLE IF EXISTS anaseq.pX_nodfi_etape2 CASCADE;
			DROP TABLE IF EXISTS anaseq.par_pZ CASCADE;
			DROP TABLE IF EXISTS anaseq.pX_non_cadastre CASCADE;
			\$\$);
		COMMIT;

		X := X + 1; -- Incrémentation de X
		pX := ((pX::numeric)+1)::text;
 
	END LOOP;

	------------------------------------------
	---Constitution de la table multi-millésime totale
	-------------------------------------------
	RAISE NOTICE 'INSERT FINAL DU DEPARTEMENT % DANS LA TABLE MULTI-MILLESIME TOTALE',dep;
	EXECUTE format(
		\$\$
		INSERT INTO anaseq.%I
		SELECT filiation_p5.idpk,
			filiations_anterieures.ccodep,
			filiations_anterieures.iddep22,
			filiations_anterieures.idcom22,
			filiations_anterieures.annee,
			filiations_anterieures.com_niv,
			filiations_anterieures.scorpat,
			filiations_anterieures.typpat,
			filiation_p5.p5_idpar,
			filiation_p4.p4_idpar,
			filiation_p3.p3_idpar,
			filiation_p2.p2_idpar,
			filiation_p1.p1_idpar,
			filiations_anterieures.m1_idpar,
			filiations_anterieures.m2_idpar,
			filiations_anterieures.m3_idpar,
			filiations_anterieures.m4_idpar,
			filiations_anterieures.m5_idpar,
			filiations_anterieures.m6_idpar,
			filiation_p5.p5_idprocpte,
			filiation_p4.p4_idprocpte,
			filiation_p3.p3_idprocpte,
			filiation_p2.p2_idprocpte,
			filiation_p1.p1_idprocpte,
			filiations_anterieures.m1_idprocpte,
			filiations_anterieures.m2_idprocpte,
			filiations_anterieures.m3_idprocpte,
			filiations_anterieures.m4_idprocpte,
			filiations_anterieures.m5_idprocpte,
			filiations_anterieures.m6_idprocpte,
			filiation_p5.geom,
			filiation_p5.area
		FROM anaseq.%I filiations_anterieures
		JOIN anaseq.filiation_p1 ON filiations_anterieures.idpk = filiation_p1.idfil_m6
		JOIN anaseq.filiation_p2 ON filiation_p1.idpk = filiation_p2.idfil_p1
		JOIN anaseq.filiation_p3 ON filiation_p2.idpk = filiation_p3.idfil_p2
		JOIN anaseq.filiation_p4 ON filiation_p3.idpk = filiation_p4.idfil_p3
		JOIN anaseq.filiation_p5 ON filiation_p4.idpk = filiation_p5.idfil_p4
		\$\$,
		table_filiation_totale_dep,
		table_filiation_anterieure_dep
		);
	COMMIT;

	EXECUTE format(
		\$\$
		DROP TABLE IF EXISTS anaseq.filiation_p1 CASCADE;
		DROP TABLE IF EXISTS anaseq.filiation_p2 CASCADE;
		DROP TABLE IF EXISTS anaseq.filiation_p3 CASCADE;
		DROP TABLE IF EXISTS anaseq.filiation_p4 CASCADE;
		DROP TABLE IF EXISTS anaseq.filiation_p5 CASCADE;
		\$\$);
	COMMIT;

END LOOP;

EXECUTE format(
	\$\$
	DROP TABLE anaseq.dep
	\$\$);

END
\$do\$
