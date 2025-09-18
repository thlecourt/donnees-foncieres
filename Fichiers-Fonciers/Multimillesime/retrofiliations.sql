--- Ce script, en l'état, reconstitue la filiation des gisements fonciers appartenant au bloc communal au 1ier janiver 2016 dans l'aire d'attraction des villes, sur 5 millésimes (2011 <- 2016)
--- m1 correspond au millésime de départ (2016) et m6 au millésime le plus ancien (2011)

DO
\$do\$
declare
dep varchar(2);
idcom22 varchar(5);
m1 text;
m1_short text;
partition_tablename text;
a_ccodep text[];
table_filiation_anterieure_dep text;

X integer := 1;---initialisation de la boucle sur la 1ère année à filier
mX text;---année à filier
mY text;---année suivante
mZ text;---année encore suivante pour les sauts de millésime
table_filiation_mX text;
table_filiation_mX2 text;
idfil_filiation_mX text;
table_filiation_mY text;
mX_idpar text;
mY_idpar text;
mZ_idpar text;
mY_idprocpte text;
mZ_idprocpte text;
nom_contrainte_mX_mY_mZ_unique text;

table_publique_m1 text;
table_publique_m1_2 text;
schema_parcelles_dep_mX text;
table_parcelles_dep_mX text;
schema_parcelles_dep_mY text;
table_parcelles_dep_mY text;
schema_parcelles_dep_mZ text;
table_parcelles_dep_mZ text;

millesime_de_reference text;
schema_de_reference text;
table_de_reference text;
table_de_reference2 text;

BEGIN


----------------------------------------
----------------------------------------
m1 := '2016';--------millésime de départ
----------------------------------------
----------------------------------------

m1_short := RIGHT(m1,2);

--------------------------
---Création des structures de table de filiation jusqu'à t-5 (m6)
--------------------------
CREATE SCHEMA IF NOT EXISTS anaseq;

--CREATION D'UNE TABLE TEMPORAIRE DES DEPARTEMENTS
DROP TABLE IF EXISTS anaseq.dep;
CREATE TABLE anaseq.dep AS (
	WITH sub AS (
		SELECT LOWER(ccodep) ccodep
		FROM nat.public16
		GROUP BY ccodep)
	SELECT ARRAY_AGG(ccodep) ccodep
	FROM sub
);
a_ccodep = (SELECT ccodep FROM anaseq.dep GROUP BY ccodep);

DROP TABLE IF EXISTS anaseq.filiations_anterieures CASCADE;
CREATE TABLE anaseq.filiations_anterieures (
	annee varchar(4),
	idpk int,
	ccodep varchar(2),
	iddep22 varchar(2),
	idcom22 varchar(5),
	com_niv varchar(2),
	scorpat int,
	typpat text,
	m1_idpar text,
	m2_idpar text,
	m3_idpar text,
	m4_idpar text,
	m5_idpar text,
	m6_idpar text,
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

CREATE INDEX filiations_anterieures_geom_idx ON anaseq.filiations_anterieures USING gist(geom);
CREATE INDEX filiations_anterieures_idcom22_idx ON anaseq.filiations_anterieures(idcom22);

	
FOREACH dep IN ARRAY ARRAY[a_ccodep]
LOOP

	RAISE NOTICE '\\\\DEPARTEMENT %////',dep;
	
	partition_tablename = 'filiations_anterieures_'||dep;
	EXECUTE format(
		\$\$ 
		CREATE TABLE IF NOT EXISTS anaseq.%I
		PARTITION OF anaseq.filiations_anterieures
		FOR VALUES IN (%L)
		\$\$, partition_tablename,dep);
	
	table_filiation_anterieure_dep := 'filiations_anterieures_'||dep;

	RAISE NOTICE '---FILIATIONS ANTERIEURES DES PARCELLES DE %', m1;
	X := 1;----(ré)initialisation de la boucle
	mX := (m1::numeric)::text;

	IF m1 IN ('2018','2019','2020') AND dep LIKE '2%%'
		THEN table_publique_m1_2 := 'public'||m1_short||'_'||dep;
	ELSIF m1 NOT IN ('2018','2019','2020') AND dep LIKE '2%%'
		THEN table_publique_m1_2 := 'public'||m1_short||'_'||UPPER(dep);
	ELSE table_publique_m1_2 := 'public'||m1_short||'_'||dep;
	END IF;
	table_publique_m1 := table_publique_m1_2;

	
	-------------------------------------------
	---Sélection des parcelles à affilier
	-------------------------------------------
	EXECUTE format(
		\$\$
		DROP TABLE IF EXISTS anaseq.com_m1 CASCADE;
		CREATE TABLE anaseq.com_m1 AS (
			SELECT %L annee,
				%L ccodep,
				pub_m1.iddep22,
				pub_m1.idcom22,---commune concernée
				pub_m1.idpk,
				pub_m1.idpar m1_idpar,---parcelle en m1
				pub_m1.idprocpte m1_idprocpte,
				pub_m1.com_niv,
				pub_m1.scorpat,
				pub_m1.typpat,
				pub_m1.lastgeompar_v geom,
				pub_m1.area area
			FROM nat.%I pub_m1
			JOIN insee.com_aav2020_2022 aav ON pub_m1.idcom22 = aav.codgeo----jointure vers les aires d'attraction
			WHERE pub_m1.cominterco IS TRUE---uniquement les parcelles communales en m1
				AND aav.typeaav20::int > 0---pas les communes hors aire attraction
				AND pub_m1.scorpat > 1---uniquement les gisements fonciers
				AND pub_m1.com_niv != 7---pas les communaux
		);
		CREATE INDEX com_m1_idpk_idx ON anaseq.com_m1(idpk);
		\$\$,
		m1,
		dep,
		table_publique_m1
		);
	COMMIT;
			
			
	LOOP

		IF X > 5 THEN
			EXIT; -- Sortir de la boucle lorsque x devient supérieur à 5 pour s'arrêter en 2011 si m1 = 2016
		END IF;

		RAISE NOTICE '-----Filiations en m%-----',(X+1)::text;

		table_filiation_mY := 'filiation_m'||(X+1)::text;
		idfil_filiation_mX := 'idfil_m'||X::text;
		mX_idpar := 'm'||(X)::text||'_idpar';
		mY_idpar := 'm'||(X+1)::text||'_idpar';
		mZ_idpar := 'm'||(X+2)::text||'_idpar';
		mY_idprocpte := 'm'||(X+1)::text||'_idprocpte';
		mZ_idprocpte := 'm'||(X+2)::text||'_idprocpte';
		nom_contrainte_mX_mY_mZ_unique := 'filiation_m'||(X)::text||'_m'||(X+1)::text||'_m'||(X+2)::text||'_unique';

		RAISE NOTICE 'Création de la structure de table';

		EXECUTE format(
			\$\$
			DROP TABLE IF EXISTS anaseq.%I CASCADE;
			CREATE TABLE anaseq.%I(
				ccodep varchar(2),
				iddep22 varchar(2),
				idcom22 varchar(5),
				annee varchar(4),---annee d'évolution initiale
				%I int,---idfil_mX
				idpk SERIAL,---idfil_mY
				%I text,---mX_idpar
				%I text,---mY_idpar
				%I text,---mZ_idpar
				%I text,---mY_idprocpte
				%I text,---mZ_idprocpte
				area numeric,
				geom geometry,
				type_evol text,
				PRIMARY KEY (idpk),
				CONSTRAINT %I UNIQUE(annee,%I,%I,%I)
			);
			\$\$,
			table_filiation_mY,
			table_filiation_mY,
			idfil_filiation_mX,
			mX_idpar,
			mY_idpar,
			mZ_idpar,
			mY_idprocpte,
			mZ_idprocpte,
			nom_contrainte_mX_mY_mZ_unique, idfil_filiation_mX, mY_idpar, mZ_idpar---la contrainte portant sur idfil_filiation_mX porte en fait sur toute la chaine précédente 
			);
		COMMIT;
	
		
		mX := (mX::numeric)::text;
		mY := ((mX::numeric)-1)::text;
		mZ := ((mX::numeric)-2)::text;
		schema_parcelles_dep_mX := 'ff_'||mX||'_dep';
		schema_parcelles_dep_mY := 'ff_'||mY||'_dep';
		schema_parcelles_dep_mZ := 'ff_'||mZ||'_dep';
		table_parcelles_dep_mX := 'd'||dep||'_'||mX||'_pnb10_parcelle';
		table_parcelles_dep_mY := 'd'||dep||'_'||mY||'_pnb10_parcelle';
		table_parcelles_dep_mZ := 'd'||dep||'_'||mZ||'_pnb10_parcelle';
		mX_idpar := 'm'||(X)::text||'_idpar';
		mY_idpar := 'm'||(X+1)::text||'_idpar';
		mZ_idpar := 'm'||(X+2)::text||'_idpar';
		mY_idprocpte := 'm'||(X+1)::text||'_idprocpte';
		mZ_idprocpte := 'm'||(X+2)::text||'_idprocpte';
		
		IF X = 1 THEN
			table_filiation_mX2 := 'com_m1';
		ELSE
			table_filiation_mX2 := 'filiation_m'||X;
		END IF;
		table_filiation_mX := table_filiation_mX2;
		table_filiation_mY := 'filiation_m'||(X+1)::text;
		idfil_filiation_mX := 'idfil_m'||X::text;
	
		
		-------------------------------------------
		----SAUTS DE MILLESIME DEJA RETROUVES A L'ETAPE PRECEDENTE
		-------------------------------------------
		IF X > 1 THEN--- à partir de m3 seulement
			RAISE NOTICE 'SAUTS DE MILLESIME DEJA RETROUVES';
			EXECUTE format(
				\$\$				
				INSERT INTO anaseq.%I(---table_filiation_mY
					ccodep,
					iddep22,
					idcom22,
					annee,
					%I,---idfil_filiation_mX
					%I,---mX_idpar
					%I,---mY_idpar
					%I,---mY_idprocpte
					%I,---mZ_idpar
					area,
					geom,
					type_evol)
				
				SELECT %L,
					iddep22,
					idcom22,
					annee,
					idpk,
					%I,---mY_idpar--> 'saut'
					%I,---mZ_idpar en tant que mY
					%I,---mZ_idprocpte en tant que mY
					'SO',---sans objet
					area,
					geom,
					'saut de millésime'
				FROM anaseq.%I---table_filiation_mX
				WHERE %I LIKE 'saut'---mY_idpar
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
				table_filiation_mY,
				idfil_filiation_mX,
				mX_idpar,
				mY_idpar,
				mY_idprocpte,
				mZ_idpar,
				---SELECT
				dep,
				mX_idpar, ---mY-1 car on est passé dans l'année suivante
				mY_idpar,---mZ-1 car on est passé dans l'année suivante
				mY_idprocpte,---mZ-1 car on est passé dans l'année suivante
				table_filiation_mX,---FROM
				mX_idpar,---mY-1 car on est passé dans l'année suivante
				m1,
				mX_idpar,---mY-1 car on est passé dans l'année suivante
				mY_idpar,---mZ-1 car on est passé dans l'année suivante
				mY_idprocpte---mZ-1 car on est passé dans l'année suivante
				);
			COMMIT;

			-------------------------------------------
			----NC sans géométrie --> maintenu en NC
			-------------------------------------------
			RAISE NOTICE 'MAINTIEN DU NC SANS GEOMETRIE';
			EXECUTE format(
				\$\$				
				INSERT INTO anaseq.%I(---table_filiation_mY
					ccodep,
					iddep22,
					idcom22,
					annee,
					%I,---idfil_filiation_mX
					%I,---mX_idpar
					%I,---mY_idpar
					%I,---mY_idprocpte
					%I,---mZ_idpar
					area,
					type_evol)
				
				SELECT %L,
					iddep22,
					idcom22,
					annee,
					idpk,
					%I,---mY_idpar--> 'NC'
					'NC',
					'NC',
					'SO',---sans objet
					area,
					'maintien en NC'
				FROM anaseq.%I---table_filiation_mX
				WHERE %I LIKE 'NC'---mY_idpar
					AND geom IS NULL
					AND annee = %L --m1
				GROUP BY iddep22,
					idcom22,
					annee,
					idpk,
					%I,
					area,
					geom
				\$\$,
				table_filiation_mY,
				idfil_filiation_mX,
				mX_idpar,
				mY_idpar,
				mY_idprocpte,
				mZ_idpar,
				---SELECT
				dep,
				mX_idpar, ---mY-1 car on est passé dans l'année suivante
				table_filiation_mX,---FROM
				mX_idpar,---mY-1 car on est passé dans l'année suivante
				m1,
				mX_idpar---mY-1 car on est passé dans l'année suivante
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
				SELECT mX.iddep22,
					mX.idcom22,
					mX.annee,
					mX.idpk,
					mX.%I,
					mY.idpar %I,
					mY.idprocpte %I,
					mX.area,
					mX.geom,
					'aucune' type_evol
				FROM anaseq.%I mX
				JOIN %I.%I mY ON mX.%I = mY.idpar
					AND mX.%I NOT LIKE 'NC'---à partir de m3 ce cas est possible
				WHERE mX.annee = %L --m1
				UNION
				SELECT mX.iddep22,
					mX.idcom22,
					mX.annee,
					mX.idpk,
					mX.%I,
					mY.idpar %I,
					mY.idprocpte %I,
					mX.area,
					mX.geom,
					'aucune' type_evol
				FROM anaseq.%I mX
				JOIN %I.%I mY ON mX.%I = mY.idpar_inverse
					AND mX.%I NOT LIKE 'NC'---à partir de m3 ce cas est possible
				WHERE mX.annee = %L --m1
			)
			
			INSERT INTO anaseq.%I(---table_filiation_mY
				ccodep,
				iddep22,
				idcom22,
				annee,
				%I,---idfil_filiation_mX
				%I,---mX_idpar
				%I,---mY_idpar
				%I,---mY_idprocpte
				%I,---mZ_idpar
				area,
				geom,
				type_evol)
			
			SELECT %L,
				iddep22,
				idcom22,
				annee,
				idpk,
				%I,---mX_idpar
				%I,---mY_idpar
				%I,---mY_idprocpte
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
			mX_idpar,
			mY_idpar,
			mY_idprocpte,
			table_filiation_mX,
			schema_parcelles_dep_mY, table_parcelles_dep_mY, mX_idpar,
			mX_idpar,
			m1,
			---UNION
			mX_idpar,
			mY_idpar,
			mY_idprocpte,
			table_filiation_mX,
			schema_parcelles_dep_mY, table_parcelles_dep_mY, mX_idpar,
			mX_idpar,
			m1,
			---INSERT
			table_filiation_mY,
			idfil_filiation_mX,
			mX_idpar,
			mY_idpar,
			mY_idprocpte,
			mZ_idpar,
			dep,
			mX_idpar,
			mY_idpar,
			mY_idprocpte,
			mX_idpar,
			mY_idpar,
			mY_idprocpte
			);
		COMMIT;


		-------------------------------------------
		----Sélection des parcelles en mX non-retrouvées en mY à ce stade (dont le NC)
		-------------------------------------------
		EXECUTE format(
			\$\$
			DROP TABLE IF EXISTS anaseq.reste_mX CASCADE;
			CREATE TABLE anaseq.reste_mX AS (
				SELECT mX.iddep22,
					mX.idcom22,
					mX.idpk idfil_mX,
					mX.%I mX_idpar,
					mX.geom mX_geom,
					mX.area mX_area
				FROM anaseq.%I mX---table des parcelles à filier en mX
				LEFT JOIN %I.%I tot_mY ON mX.%I = tot_mY.idpar
				LEFT JOIN %I.%I tot_mYbis ON mX.%I = tot_mYbis.idpar_inverse
				WHERE mX.annee = %L
					AND tot_mY.idpar IS NULL ---qu'on ne retrouve pas en mY, ni en idpar normal
					AND tot_mYbis.idpar_inverse IS NULL ---ni en idpar_inverse
				GROUP BY mX.iddep22,
					mX.idcom22,
					idfil_mX,
					mX_idpar,
					mX_geom,
					mX_area);
			\$\$,
			mX_idpar,
			table_filiation_mX,
			schema_parcelles_dep_mY, table_parcelles_dep_mY, mX_idpar,
			schema_parcelles_dep_mY, table_parcelles_dep_mY, mX_idpar,
			m1);
		COMMIT;

		---Table des parcelles qu'on ne retrouve pas dans les DFI, pour traitement ultérieur (dont le NC)
		EXECUTE format(
			\$\$
			DROP TABLE IF EXISTS anaseq.reste_mX_nodfi CASCADE;
			CREATE TABLE anaseq.reste_mX_nodfi AS (
				SELECT mX.iddep22,
					mX.idcom22,
					mX.idfil_mX,
					mX.mX_idpar,
					mX.mX_geom,
					mX.mX_area
				FROM anaseq.reste_mX mX
				LEFT JOIN dfi.unnest dfi ON mX.mX_idpar = dfi.idpar_fille_normal
					AND EXTRACT(YEAR FROM dfi.date) BETWEEN %s AND %s
					AND mX.mX_idpar NOT LIKE 'NC'
				LEFT JOIN dfi.unnest dfi_bis ON mX.mX_idpar = dfi_bis.idpar_fille_inverse
					AND EXTRACT(YEAR FROM dfi_bis.date) BETWEEN %s AND %s
					AND mX.mX_idpar NOT LIKE 'NC'
				WHERE dfi.idpar_fille_normal IS NULL
					AND dfi_bis.idpar_fille_inverse IS NULL
					AND mX.mX_idpar NOT LIKE 'saut');---on a déjà retrouvé les sauts de millésime
			\$\$,
			mY,mX,
			mY,mX);
		COMMIT;


		-------------------------------------------
		---Traitement des évolutions simples dans les DFI
		-------------------------------------------
		RAISE NOTICE 'EVOLUTIONS SIMPLES';

		---Table de passage entre reste_mX et DFI
		EXECUTE format(
			\$\$
			DROP TABLE IF EXISTS anaseq.reste_mX_dfi CASCADE;
			CREATE TABLE anaseq.reste_mX_dfi AS (
				SELECT dfi.date,
					dfi.id_filiation,
					dfi.type,
					mX.iddep22,
					mX.idcom22,
					mX.idfil_mX,
					mX.mX_idpar, --- parcelles restantes à filier
					mX.mX_geom,
					mX.mX_area,
					dfi.idpar_fille_normal mX_idpar_fille_normal,
					dfi.idpar_mere_normal mX_idpar_mere_normal,
					dfi.idpar_mere_inverse mX_idpar_mere_inverse
				FROM anaseq.reste_mX mX
				JOIN dfi.unnest dfi ON mX.mX_idpar = dfi.idpar_fille_normal---dont on retrouve l'id dans les idpar_fille des DFI
					AND EXTRACT(YEAR FROM date) BETWEEN %s AND %s
					AND mX.mX_idpar NOT LIKE 'NC'
				GROUP BY
					dfi.date,
					dfi.id_filiation,
					dfi.type,
					mX.iddep22,
					mX.idcom22,
					idfil_mX,
					mX_idpar,
					mX_geom,
					mX_area,
					mX_idpar_fille_normal,
					mX_idpar_mere_normal,
					mX_idpar_mere_inverse
				UNION
				SELECT dfi.date,
					dfi.id_filiation,
					dfi.type,
					mX.iddep22,
					mX.idcom22,
					mX.idfil_mX,
					mX.mX_idpar, --- parcelles restantes à filier
					mX.mX_geom,
					mX.mX_area,
					dfi.idpar_fille_normal mX_idpar_fille_normal,
					dfi.idpar_mere_normal mX_idpar_mere_normal,
					dfi.idpar_mere_inverse mX_idpar_mere_inverse
				FROM anaseq.reste_mX mX
				JOIN dfi.unnest dfi ON mX.mX_idpar = dfi.idpar_fille_inverse---dont on retrouve l'id dans les idpar_fille inverse des DFI
					AND EXTRACT(YEAR FROM date) BETWEEN %s AND %s
				GROUP BY
					dfi.date,
					dfi.id_filiation,
					dfi.type,
					mX.iddep22,
					mX.idcom22,
					idfil_mX,
					mX_idpar,
					mX_geom,
					mX_area,
					mX_idpar_fille_normal,
					mX_idpar_mere_normal,
					mX_idpar_mere_inverse);
			DROP TABLE IF EXISTS anaseq.reste_mx CASCADE;
			\$\$,
			mY,mX,
			mY,mX);
		COMMIT;
		
		---Insert de la filiation si mY = NC (parcelle originaire du NC)
		EXECUTE format(
			\$\$
			INSERT INTO anaseq.%I(
				ccodep,
				iddep22,
				idcom22,
				annee,
				%I,---idfil_mX
				%I,---idpar_mX
				%I,---idpar_mY
				%I,---idprocpte_mY
				%I,---idpar_mZ
				area,
				geom,
				type_evol)
			
			SELECT %L,
				iddep22,
				idcom22,
				%L,---annee d'observation initiale
				idfil_mX,
				mX_idpar,
				mX_idpar_mere_normal,----NC
				mX_idpar_mere_normal,----NC
				'SO',---sans objet
				mX_area,
				mX_geom,
				'sortie de NC simple'
			FROM anaseq.reste_mX_dfi
			WHERE mX_idpar_mere_normal LIKE 'NC'
			GROUP BY iddep22,
				idcom22,
				idfil_mX,
				mX_idpar,
				mX_area,
				mX_geom,
				mX_idpar_mere_normal;

			DELETE FROM anaseq.reste_mX_dfi WHERE mX_idpar_mere_normal LIKE 'NC';
			\$\$,
			table_filiation_mY,
			idfil_filiation_mX,
			mX_idpar,
			mY_idpar,
			mY_idprocpte,
			mZ_idpar,
			dep,
			m1);
		COMMIT;

		---Recherche de parcelles-mères correspondantes dans les Fichiers Fonciers en mY pour les autres
		EXECUTE format(
			\$\$
			DROP TABLE IF EXISTS anaseq.reste_mX_dfi_tot_mY CASCADE;
			CREATE TABLE anaseq.reste_mX_dfi_tot_mY AS (
				SELECT mX.id_filiation,
					mX.date,
					mX.type type_evol,
					mX.iddep22,
					mX.idcom22,
					mX.idfil_mX,
					mX.mX_idpar,
					mX.mX_geom,
					mX.mX_area,
					mX.mX_idpar_fille_normal,
					mX_idpar_mere_normal,
					mX_idpar_mere_inverse,
					CASE
						WHEN tot_mY.idpar IS NULL AND tot_mYbis.idpar IS NULL THEN tot_mYter.idpar
						WHEN tot_mY.idpar IS NULL AND tot_mYbis.idpar IS NOT NULL THEN tot_mYbis.idpar
						ELSE tot_mY.idpar
					END mY_idpar_tot_mY,
					CASE
						WHEN tot_mY.idpar IS NULL AND tot_mYbis.idpar IS NULL THEN tot_mYter.dcntpa
						WHEN tot_mY.idpar IS NULL AND tot_mYbis.idpar IS NOT NULL THEN tot_mYbis.dcntpa
						ELSE tot_mY.dcntpa
					END mY_area_tot_mY,
					CASE
						WHEN tot_mY.idpar IS NULL AND tot_mYbis.idpar IS NULL THEN tot_mYter.idprocpte
						WHEN tot_mY.idpar IS NULL AND tot_mYbis.idpar IS NOT NULL THEN tot_mYbis.idprocpte
						ELSE tot_mY.idprocpte
					END mY_idprocpte_tot_mY
				FROM anaseq.reste_mX_dfi mX
				LEFT JOIN %I.%I tot_mY ON mX.mX_idpar_mere_normal = tot_mY.idpar---on regarde si on retrouve les idparmere dans les FF ou non
				LEFT JOIN %I.%I tot_mYbis ON mX.mX_idpar_mere_normal = tot_mYbis.idpar_inverse---on regarde si on retrouve les idparmere dans les FF ou non
				LEFT JOIN %I.%I tot_mYter ON mX.mX_idpar_mere_inverse = tot_mYter.idpar---on regarde si on retrouve les idparmere dans les FF ou non
				GROUP BY mX.id_filiation,
					mX.date,
					mX.type,
					mX.iddep22,
					mX.idcom22,
					idfil_mX,
					mX_idpar,
					mX_geom,
					mX_area,
					mX.mX_idpar_fille_normal,
					mX_idpar_mere_normal,
					mX_idpar_mere_inverse,
					mY_idpar_tot_mY,
					mY_area_tot_mY,
					mY_idprocpte_tot_mY);
			DROP TABLE IF EXISTS anaseq.reste_mX_dfi CASCADE;
			\$\$,
			schema_parcelles_dep_mY, table_parcelles_dep_mY,
			schema_parcelles_dep_mY, table_parcelles_dep_mY,
			schema_parcelles_dep_mY, table_parcelles_dep_mY);
		COMMIT;

		---Table de passage intermédiaire mX_mY
		EXECUTE format(
			\$\$
			DROP TABLE IF EXISTS anaseq.passage_mX_mY CASCADE;
			CREATE TABLE anaseq.passage_mX_mY(
					iddep22 varchar(2),
					idcom22 varchar(5),
					idfil_mX int,
					mX_idpar text,
					mX_geom geometry,
					mX_area numeric,
					mY_idpar text,
					mY_area numeric,
					mY_idprocpte text,
					type_evol text);
			\$\$);
		COMMIT;

		---Insert dans la table de passage géométrique si filiation entièrement retrouvée en évolution simple
		EXECUTE format(
			\$\$				
			WITH sub AS (
				SELECT mX.mX_idpar,
					COUNT(*) FILTER (WHERE mY_idpar_tot_mY IS NULL) as nb_par_manquantes
				FROM anaseq.reste_mX_dfi_tot_mY mX
				GROUP BY mX.mX_idpar),
						
			sub2 AS (
				SELECT iddep22,
					idcom22,
					mX.idfil_mX,
					mX.mX_idpar,
					mX_geom,
					mX_area,
					mY_idpar_tot_mY mY_idpar,
					mY_area_tot_mY mY_area,
					mY_idprocpte_tot_mY mY_idprocpte,
					type_evol
				FROM anaseq.reste_mX_dfi_tot_mY mX
				JOIN sub ON sub.mX_idpar = mX.mX_idpar
					AND sub.nb_par_manquantes = 0
				GROUP BY iddep22,
					idcom22,
					idfil_mX,
					mX.mX_idpar,
					mX_geom,
					mX_area,
					mY_idpar,
					mY_area,
					mY_idprocpte,
					type_evol)

			INSERT INTO anaseq.passage_mX_mY
				SELECT iddep22,
					idcom22,
					idfil_mX,
					mX_idpar,
					mX_geom,
					mX_area,
					mY_idpar,
					mY_area,
					mY_idprocpte,
					type_evol
				FROM sub2
				GROUP BY iddep22,
					idcom22,
					idfil_mX,
					mX_idpar,
					mX_geom,
					mX_area,
					mY_idpar,
					mY_area,
					mY_idprocpte,
					type_evol
			\$\$);
		COMMIT;

		---On supprime des parcelles à filier toutes celles qu'on a retrouvées entièrement en évolution simple
		EXECUTE format(
			\$\$
			DELETE FROM anaseq.reste_mX_dfi_tot_mY 
			WHERE mX_idpar IN
				(SELECT mX_idpar
				FROM anaseq.reste_mX_dfi_tot_mY 
				GROUP BY mX_idpar
				HAVING COUNT(*) FILTER (WHERE mY_idpar_tot_mY IS NULL) = 0);
			\$\$);
		COMMIT;


		-------------------------------------------
		---Traitement des évolutions complexes
		-------------------------------------------
		RAISE NOTICE 'EVOLUTIONS COMPLEXES';

		---Table à 3 niveaux (trop lourd au-delà et peu d'impact) des filiations sur 1 à 3 DFI consécutifs
		EXECUTE format(
			\$\$
			DROP TABLE IF EXISTS anaseq.mX_complexe CASCADE;
			CREATE TABLE anaseq.mX_complexe AS (
				WITH sub AS (
					SELECT mX.iddep22,
						mX.idcom22,
						mX.date date1,
						dfi.date date2,
						mX.idfil_mX,
						mX.mX_idpar idpar0,
						mX.mX_geom geom0,
						mX.mX_area area0,
						mX.mX_idpar_mere_normal idpar1,
						mX.mY_idpar_tot_mY idpar1_tot_mY,
						mX.mY_area_tot_mY area1_tot_mY,
						mX.mY_idprocpte_tot_mY idprocpte1_tot_mY,
						dfi.idpar_mere_normal idpar2,---on garde les idpar mere de 2e niveau
						CASE
							WHEN tot_mY.idpar IS NULL AND tot_mYbis.idpar IS NULL THEN tot_mYter.idpar
							WHEN tot_mY.idpar IS NULL AND tot_mYbis.idpar IS NOT NULL THEN tot_mYbis.idpar
							ELSE tot_mY.idpar
						END idpar2_tot_mY,
						CASE
							WHEN tot_mY.idpar IS NULL AND tot_mYbis.idpar IS NULL THEN tot_mYter.dcntpa
							WHEN tot_mY.idpar IS NULL AND tot_mYbis.idpar IS NOT NULL THEN tot_mYbis.dcntpa
							ELSE tot_mY.dcntpa
						END area2_tot_mY,
						CASE
							WHEN tot_mY.idpar IS NULL AND tot_mYbis.idpar IS NULL THEN tot_mYter.idprocpte
							WHEN tot_mY.idpar IS NULL AND tot_mYbis.idpar IS NOT NULL THEN tot_mYbis.idprocpte
							ELSE tot_mY.idprocpte
						END idprocpte2_tot_mY
					FROM anaseq.reste_mX_dfi_tot_mY mX
					LEFT JOIN dfi.unnest dfi ON dfi.idpar_fille_normal = mX.mX_idpar_mere_normal
						AND mX.mY_idpar_tot_mY IS NULL
						AND dfi.date <= mX.date---seulement si la filiation est antérieure
					LEFT JOIN %I.%I tot_mY ON dfi.idpar_mere_normal = tot_mY.idpar---on check si on retrouve une parcelle en mY
						AND mX.mY_idpar_tot_mY IS NULL---jointure uniquement si géométrie pas déjà retrouvée
						AND dfi.idpar_mere_normal NOT LIKE 'NC' --- sauf si originaire du non-cadastré
					LEFT JOIN %I.%I tot_mYbis ON dfi.idpar_mere_normal = tot_mYbis.idpar_inverse---on regarde si on retrouve les idparmere dans les parcelles de m1 ou non
						AND mX.mY_idpar_tot_mY IS NULL
						AND dfi.idpar_mere_normal NOT LIKE 'NC' --- sauf si originaire du non-cadastré
					LEFT JOIN %I.%I tot_mYter ON dfi.idpar_mere_inverse = tot_mYter.idpar
						AND mX.mY_idpar_tot_mY IS NULL
						AND dfi.idpar_mere_normal NOT LIKE 'NC' --- sauf si originaire du non-cadastré
					GROUP BY date1,
						date2,
						mX.iddep22,
						mX.idcom22,
						idfil_mX,
						idpar0,
						geom0,
						area0,
						idpar1,
						idpar1_tot_mY,
						area1_tot_mY,
						idprocpte1_tot_mY,
						idpar2,
						idpar2_tot_mY,
						area2_tot_mY,
						idprocpte2_tot_mY)
				
				SELECT sub.date1,
					sub.date2,
					dfi.date date3,
					sub.iddep22,
					sub.idcom22,
					sub.idfil_mX,
					sub.idpar0,
					sub.geom0,
					sub.area0,
					sub.idpar1,
					sub.idpar1_tot_mY,
					sub.area1_tot_mY,
					sub.idprocpte1_tot_mY,
					sub.idpar2,
					sub.idpar2_tot_mY,
					sub.area2_tot_mY,
					sub.idprocpte2_tot_mY,
					dfi.idpar_mere_normal idpar3,----- et les idpar_mere de 3e niveau
					CASE
						WHEN tot_mY.idpar IS NULL AND tot_mYbis.idpar IS NULL THEN tot_mYter.idpar
						WHEN tot_mY.idpar IS NULL AND tot_mYbis.idpar IS NOT NULL THEN tot_mYbis.idpar
						ELSE tot_mY.idpar
					END idpar3_tot_mY,
					CASE
						WHEN tot_mY.idpar IS NULL AND tot_mYbis.idpar IS NULL THEN tot_mYter.dcntpa
						WHEN tot_mY.idpar IS NULL AND tot_mYbis.idpar IS NOT NULL THEN tot_mYbis.dcntpa
						ELSE tot_mY.dcntpa
					END area3_tot_mY,
					CASE
						WHEN tot_mY.idpar IS NULL AND tot_mYbis.idpar IS NULL THEN tot_mYter.idprocpte
						WHEN tot_mY.idpar IS NULL AND tot_mYbis.idpar IS NOT NULL THEN tot_mYbis.idprocpte
						ELSE tot_mY.idprocpte
					END idprocpte3_tot_mY
				FROM sub
				LEFT JOIN dfi.unnest dfi ON dfi.idpar_fille_normal = sub.idpar2
					AND dfi.date <= sub.date2---seulement si la filiation est antérieure
					AND sub.idpar2_tot_mY IS NULL
					AND sub.idpar2 NOT LIKE 'NC'---pas de filiation supplémentaire dans les DFI si originaire du NC
				LEFT JOIN %I.%I tot_mY ON dfi.idpar_mere_normal = tot_mY.idpar---on check si on retrouve une parcelle en mY
					AND sub.idpar2_tot_mY IS NULL---jointure uniquement si géométrie pas déjà retrouvée
					AND dfi.idpar_mere_normal NOT LIKE 'NC' --- sauf si originaire du non-cadastré
				LEFT JOIN %I.%I tot_mYbis ON dfi.idpar_mere_normal = tot_mYbis.idpar_inverse---on regarde si on retrouve les idparfille dans les parcelles de 2018 ou non
					AND dfi.idpar_mere_normal NOT LIKE 'NC' --- sauf si originaire du non-cadastré
				LEFT JOIN %I.%I tot_mYter ON dfi.idpar_mere_inverse = tot_mYter.idpar
					AND sub.idpar2_tot_mY IS NULL
					AND dfi.idpar_mere_normal NOT LIKE 'NC' --- sauf si originaire du non-cadastré
				GROUP BY date1,
					date2,
					date3,
					sub.iddep22,
					sub.idcom22,
					sub.idfil_mX,
					idpar0,
					geom0,
					area0,
					idpar1,
					idpar1_tot_mY,
					area1_tot_mY,
					idprocpte1_tot_mY,
					idpar2,
					idpar2_tot_mY,
					area2_tot_mY,
					idprocpte2_tot_mY,
					idpar3,
					idpar3_tot_mY,
					area3_tot_mY,
					idprocpte3_tot_mY
				);
			DROP TABLE anaseq.reste_mX_dfi_tot_mY CASCADE;
			\$\$,
			schema_parcelles_dep_mY, table_parcelles_dep_mY,
			schema_parcelles_dep_mY, table_parcelles_dep_mY,
			schema_parcelles_dep_mY, table_parcelles_dep_mY,
			schema_parcelles_dep_mY, table_parcelles_dep_mY,
			schema_parcelles_dep_mY, table_parcelles_dep_mY,
			schema_parcelles_dep_mY, table_parcelles_dep_mY);
		COMMIT;

		---Extraction et élagage des filiations complexes qu'on retrouve dans les Fichiers Fonciers ou en NC
		EXECUTE format(
			\$\$
			DROP TABLE IF EXISTS anaseq.mX_complexe_ff CASCADE;
			CREATE TABLE anaseq.mX_complexe_ff AS (
				SELECT iddep22,
					idcom22,
					idfil_mX,
					idpar0,
					geom0,
					area0,
					CASE
						WHEN idpar1 LIKE 'NC'
							OR idpar2 LIKE 'NC'
							OR idpar3 LIKE 'NC'
								THEN 'NC'
						WHEN idpar3_tot_mY IS NOT NULL THEN idpar3_tot_mY
						WHEN idpar2_tot_mY IS NOT NULL THEN idpar2_tot_mY
						WHEN idpar1_tot_mY IS NOT NULL THEN idpar1_tot_mY
						ELSE NULL
					END idpar_new,
					CASE
						WHEN idpar1 LIKE 'NC'
							OR idpar2 LIKE 'NC'
							OR idpar3 LIKE 'NC'
								THEN 'NC'
						WHEN idpar3_tot_mY IS NOT NULL THEN idprocpte3_tot_mY
						WHEN idpar2_tot_mY IS NOT NULL THEN idprocpte2_tot_mY
						WHEN idpar1_tot_mY IS NOT NULL THEN idprocpte1_tot_mY
						ELSE NULL
					END idprocpte_new,
					CASE
						WHEN idpar1 LIKE 'NC'
							OR idpar2 LIKE 'NC'
							OR idpar3 LIKE 'NC'
								THEN area0
						WHEN idpar3_tot_mY IS NOT NULL THEN area3_tot_mY
						WHEN idpar2_tot_mY IS NOT NULL THEN area2_tot_mY
						WHEN idpar1_tot_mY IS NOT NULL THEN area1_tot_mY
						ELSE NULL
					END area_new
				FROM anaseq.mX_complexe);
			\$\$);
		COMMIT;

		---Insert dans la table de filiation des évolutions complexes qui proviennent du NC
		EXECUTE format(
			\$\$
			INSERT INTO anaseq.%I(
				ccodep,
				iddep22,
				idcom22,
				annee,
				%I,---idfil_mX
				%I,---idpar_mX
				%I,---idpar_mY
				%I,---idprocpte_mY
				%I,---idpar_mZ
				area,
				geom,
				type_evol)
			
			SELECT %L,
				iddep22,
				idcom22,
				%L,---annee d'évolution initiale
				idfil_mX,
				idpar0,
				idpar_new,----NC
				'NC',
				'SO',---sans objet
				area0,
				geom0,
				'sortie de NC complexe'
			FROM anaseq.mX_complexe_ff
			WHERE idpar_new LIKE 'NC'
			GROUP BY iddep22,
				idcom22,
				idfil_mX,
				idpar0,
				idpar_new,
				area0,
				geom0;

			DELETE FROM anaseq.mx_complexe comp
			USING anaseq.mX_complexe_ff ff
			WHERE ff.idpar0 = comp.idpar0
				AND ff.idpar_new LIKE 'NC';
			\$\$,
			table_filiation_mY,
			idfil_filiation_mX,
			mX_idpar,
			mY_idpar,
			mY_idprocpte,
			mZ_idpar,
			dep,
			m1);
		COMMIT;

		---Insert dans la table de passage intermédiaire des autres évolutions mX_mY
		EXECUTE format(
			\$\$			
			INSERT INTO anaseq.passage_mX_mY
				SELECT comp.iddep22,
					comp.idcom22,
					comp.idfil_mX,
					comp.idpar0,
					comp.geom0,
					comp.area0,
					comp.idpar_new,
					comp.area_new,
					comp.idprocpte_new,
					'évolution complexe' type_evol
				FROM anaseq.mX_complexe_ff comp
				WHERE idpar_new NOT LIKE 'NC'
				GROUP BY comp.iddep22,
					comp.idcom22,
					comp.idfil_mX,
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

		---Correction géométrique mutlimillésime dans la table de passage mX_mY
		EXECUTE format(
			\$\$
			---Création des colonnes et index
			ALTER TABLE anaseq.passage_mX_mY DROP COLUMN IF EXISTS mY_lastgeompar CASCADE;
			ALTER TABLE anaseq.passage_mX_mY ADD COLUMN mY_lastgeompar geometry;
			CREATE INDEX mX_passage_geom_idx ON anaseq.passage_mX_mY USING gist(mX_geom);
			CREATE INDEX mY_lastgeompar_idx ON anaseq.passage_mX_mY USING gist(mY_lastgeompar);
			CREATE INDEX mX_passage_idpar_new_idx ON anaseq.passage_mX_mY(mY_idpar);
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

			IF millesime_de_reference::numeric BETWEEN mZ::numeric-1 AND mX::numeric THEN---On ne recherche lastgeompar que 2 ans avant et 1 an après l'année considérée (sinon on l'aurait déjà en cas simple)
	
				EXECUTE format(
					\$\$		
					UPDATE anaseq.passage_mX_mY a
					SET mY_lastgeompar = ST_MakeValid(b.geompar)
					FROM %I.%I b
					WHERE a.mY_lastgeompar IS NULL
						AND b.geompar IS NOT NULL
						AND a.mY_idpar = b.idpar
						AND b.vecteur LIKE 'V'; --- uniquement les géométries vectorisées

					UPDATE anaseq.passage_mX_mY a
					SET mY_lastgeompar = ST_MakeValid(b.geompar)
					FROM %I.%I b
					WHERE a.mY_lastgeompar IS NULL
						AND b.geompar IS NOT NULL
						AND a.mY_idpar = b.idpar_inverse---si changement communal
						AND b.vecteur LIKE 'V'; --- uniquement les géométries vectorisées
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
				%I,---idfil_mX
				%I,---idpar_mX
				%I,---idpar_mY
				%I,---idprocpte_mY
				%I,---idpar_mZ
				geom,
				type_evol
				)
			SELECT %L,
				sub.iddep22,
				sub.idcom22,
				%L,---année d'évolution initiale
				sub.idfil_mX,
				sub.mX_idpar,
				sub.mY_idpar,
				sub.mY_idprocpte,
				'SO',---sans objet en mZ
				ST_Union(ST_CollectionExtract(ST_MakeValid(ST_Intersection(sub.mX_geom,sub.mY_lastgeompar)),3)),
				type_evol	
			FROM anaseq.passage_mX_mY sub
			WHERE sub.mX_geom IS NOT NULL
				AND sub.mY_lastgeompar IS NOT NULL
			GROUP BY sub.iddep22,
				sub.idcom22,
				sub.idfil_mX,
				sub.mX_idpar,
				sub.mY_idpar,
				sub.mY_idprocpte,
				sub.type_evol;
			UPDATE anaseq.%I SET area = ST_Area(geom) WHERE area IS NULL;
			\$\$,
			table_filiation_mY,
			idfil_filiation_mX,
			mX_idpar,
			mY_idpar,
			mY_idprocpte,
			mZ_idpar,
			dep,
			m1,
			table_filiation_mY);
		COMMIT;

		---Insert des non-superpositions géométriques dans la table nodfi
		EXECUTE format(
			\$\$
			DROP TABLE IF EXISTS anaseq.fil_mY_uni CASCADE;
			CREATE TABLE anaseq.fil_mY_uni AS (
				SELECT ST_Union(ST_MakeValid(ST_CollectionExtract(geom,3))) geom
				FROM anaseq.%I
				WHERE annee = %L
					AND type_evol NOT LIKE 'aucune'
					AND type_evol NOT LIKE 'passage%%'
					AND type_evol NOT LIKE 'saut%%'
					AND type_evol NOT LIKE 'maintien%%'
			);
			CREATE INDEX fil_mY_uni_geom_idx ON anaseq.fil_mY_uni USING gist(geom);
			
			DROP TABLE IF EXISTS anaseq.passuni CASCADE;
			CREATE TABLE anaseq.passuni AS (
				SELECT ST_Union(ST_MakeValid(ST_CollectionExtract(mX_geom,3))) geom
				FROM anaseq.passage_mX_mY
				WHERE mX_geom IS NOT NULL
					AND mY_lastgeompar IS NOT NULL
			);
			CREATE INDEX passuni_geom_idx ON anaseq.passuni USING gist(geom);
			
			DROP TABLE IF EXISTS anaseq.diff CASCADE;
			\$\$,
			table_filiation_mY,
			m1);
			
		BEGIN
			CREATE TABLE anaseq.diff AS (
				SELECT ST_MakeValid(ST_CollectionExtract(ST_Difference(passuni.geom, fil_mY_uni.geom),3)) geom
				FROM anaseq.passuni,anaseq.fil_mY_uni);
			CREATE INDEX diff_geom_idx ON anaseq.diff USING gist(geom);

		EXCEPTION----En cas d'erreur de différence spatiale, on reproduit ST_MemUnion sur la table anaseq.fil_mY_uni
			WHEN OTHERS THEN
				
				EXECUTE format(
				\$\$
					DROP TABLE IF EXISTS anaseq.fil_mY_uni CASCADE;
					CREATE TABLE anaseq.fil_mY_uni AS (
						SELECT ST_MemUnion(ST_MakeValid(ST_CollectionExtract(geom,3))) geom
						FROM anaseq.%I
						WHERE annee = %L
							AND type_evol NOT LIKE 'aucune'
							AND type_evol NOT LIKE 'passage%%'
							AND type_evol NOT LIKE 'saut%%'
							AND type_evol NOT LIKE 'maintien%%'
					);
					CREATE INDEX fil_mY_uni_geom_idx ON anaseq.fil_mY_uni USING gist(geom);
					
					DROP TABLE IF EXISTS anaseq.diff CASCADE;
					CREATE TABLE anaseq.diff AS (
						SELECT ST_MakeValid(ST_CollectionExtract(ST_Difference(passuni.geom, fil_mY_uni.geom),3)) geom
						FROM anaseq.passuni,anaseq.fil_mY_uni);
					CREATE INDEX diff_geom_idx ON anaseq.diff USING gist(geom);
				\$\$,
				table_filiation_mY,
				m1);

			
		END;

		EXECUTE format(
			\$\$	
			INSERT INTO anaseq.reste_mX_nodfi(
				iddep22,
				idcom22,
				idfil_mX,
				mX_idpar,
				mX_geom)
			SELECT pass.iddep22,
				pass.idcom22,
				pass.idfil_mX,
				pass.mX_idpar,
				ST_Union(ST_MakeValid(ST_CollectionExtract(ST_Intersection(diff.geom,pass.mX_geom))),3)
			FROM anaseq.passage_mX_mY pass, anaseq.diff
			WHERE pass.mX_geom IS NOT NULL
			GROUP BY pass.iddep22,
				pass.idcom22,
				pass.idfil_mX,
				pass.mX_idpar;
			
			UPDATE anaseq.reste_mX_nodfi
			SET mX_area = ST_Area(mX_geom)
			WHERE mX_area IS NULL
				AND mX_geom IS NOT NULL;

			DROP TABLE anaseq.fil_mY_uni CASCADE;
			DROP TABLE anaseq.passuni CASCADE;
			DROP TABLE anaseq.diff CASCADE;
			\$\$);
		COMMIT;

		---On compte pour chaque portion de parcelle en mX la surface non-retrouvée géométriquement
		---> pour distibuer équitablement la surface restante entre les parcelles filles/mères
		EXECUTE format(
			\$\$
			ALTER TABLE anaseq.passage_mX_mY ADD COLUMN surf_geom_manquante numeric;

			WITH sub AS(
				SELECT %I,
					SUM(area) area_retrouvee
				FROM anaseq.%I
				WHERE annee = %L
				GROUP BY %I
			)
			UPDATE anaseq.passage_mX_mY a
			SET surf_geom_manquante = COALESCE(COALESCE(a.mX_area,0)-COALESCE(sub.area_retrouvee,0),0)
			FROM sub
			WHERE a.mX_idpar = sub.%I;

			UPDATE anaseq.passage_mX_mY
			SET surf_geom_manquante = mX_area
			WHERE surf_geom_manquante IS NULL
			\$\$,
			mX_idpar,
			table_filiation_mY,
			m1,
			mX_idpar,
			mX_idpar);
		COMMIT;

		---Table des parcelles avec lacune géométrique
		EXECUTE format(
			\$\$
			DROP TABLE IF EXISTS anaseq.passage_mX_mY_lacunesgeom CASCADE;
			CREATE TABLE anaseq.passage_mX_mY_lacunesgeom AS (
				SELECT * FROM anaseq.passage_mX_mY
				WHERE mX_geom IS NULL
					OR mY_lastgeompar IS NULL);
			\$\$);
		COMMIT;

		---Calcul du nombre de mères et de filles
		EXECUTE format(
			\$\$
			ALTER TABLE anaseq.passage_mX_mY_lacunesgeom ADD COLUMN nb_meres int;
			ALTER TABLE anaseq.passage_mX_mY_lacunesgeom ADD COLUMN nb_filles int;

			WITH sub AS(
				SELECT mX_idpar, COUNT(DISTINCT mY_idpar) nb_meres
				FROM anaseq.passage_mX_mY_lacunesgeom
				GROUP BY mX_idpar)
			UPDATE anaseq.passage_mX_mY_lacunesgeom a
			SET nb_meres = sub.nb_meres
			FROM sub
			WHERE a.mX_idpar = sub.mX_idpar;

			WITH sub AS (
				SELECT mY_idpar, COUNT(DISTINCT mX_idpar) nb_filles
				FROM anaseq.passage_mX_mY_lacunesgeom
				GROUP BY mY_idpar			
			)
			UPDATE anaseq.passage_mX_mY_lacunesgeom a
			SET nb_filles = sub.nb_filles
			FROM sub
			WHERE a.mY_idpar = sub.mY_idpar;
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
				%I,---idfil_mX
				%I,---idpar_mX
				%I,---idpar_mY
				%I,---idprocpte_mY
				%I,---idpar_mZ
				geom,
				area,
				type_evol
				)
			SELECT %L,
				iddep22,
				idcom22,
				%L,---année d'évolution initiale
				idfil_mX,
				mX_idpar,
				mY_idpar,
				mY_idprocpte,
				'SO',---sans objet en mZ
				CASE
					WHEN type_evol LIKE 'division'
						OR type_evol LIKE 'transfert'
						OR (nb_meres = 1 AND nb_filles >= 1)
							THEN mX_geom
					ELSE NULL::geometry
				END,
				CASE
					WHEN type_evol LIKE 'division'
						OR type_evol LIKE 'transfert'
						OR (nb_meres = 1 AND nb_filles >= 1)
							THEN mX_area
					ELSE surf_geom_manquante/(nb_meres+nb_filles-1)---divise la surface par le nombre de portions
				END,
				CONCAT(type_evol, ' (avec géométrie invalide)')
			FROM anaseq.passage_mX_mY_lacunesgeom sub
			WHERE mX_geom IS NULL
				OR mY_lastgeompar IS NULL
			GROUP BY iddep22,
				idcom22,
				idfil_mX,
				mX_idpar,
				mY_idpar,
				mY_idprocpte,
				nb_meres,
				nb_filles,
				mX_geom,
				mX_area,
				surf_geom_manquante,
				type_evol;
			\$\$,
			table_filiation_mY,
			idfil_filiation_mX,
			mX_idpar,
			mY_idpar,
			mY_idprocpte,
			mZ_idpar,
			dep,
			m1);
		COMMIT;

		---Suppression des lignes reconstituées par filiation complexe
		EXECUTE format(
			\$\$
			DELETE FROM anaseq.mX_complexe comp
			USING anaseq.passage_mX_mY pass
			WHERE comp.idpar0 = pass.mX_idpar AND
				(comp.idpar1 = pass.mY_idpar
				OR comp.idpar2 = pass.mY_idpar
				OR comp.idpar3 = pass.mY_idpar);
			
			DROP TABLE anaseq.mX_complexe_ff CASCADE;
			DROP TABLE anaseq.passage_mX_mY CASCADE;
			DROP TABLE anaseq.passage_mX_mY_lacunesgeom CASCADE;
			\$\$);
		COMMIT;


		-------------------------------------------
		---Traitement des sauts de millésime pour les parcelles restantes
		-------------------------------------------

		IF X >= 5 THEN---Si m1 = 2016, on ne peut pas retrouver de saut de millésime en 2010 (millésime manquant)
			---Insert dans la table des non-DFI des parcelles non-retrouvées
			EXECUTE format(
				\$\$				
				INSERT INTO anaseq.reste_mx_nodfi
					SELECT iddep22,
						idcom22,
						idfil_mX,
						idpar0,
						geom0,
						area0
					FROM anaseq.mX_complexe;
				DROP TABLE anaseq.mX_complexe CASCADE;
				\$\$);
			COMMIT;		

		ELSE
			RAISE NOTICE 'SAUTS DE MILLESIME';
			
			----Sélection des parcelles dont on retrouve l identifiant en mZ
			EXECUTE format(
				\$\$
				DROP TABLE IF EXISTS anaseq.mX_sauts_millesimes CASCADE;
				CREATE TABLE anaseq.mX_sauts_millesimes AS (
					
					----sans changement d'idpar
					WITH sub1 AS (
						SELECT comp.iddep22,
							comp.idcom22,
							comp.idfil_mX,
							comp.idpar0,
							comp.geom0,
							comp.area0,
							comp.idpar1,
							comp.idpar2,
							comp.idpar3,
							CASE
								WHEN tot_mZ.idpar IS NULL AND tot_mZbis.idpar IS NOT NULL THEN tot_mZbis.idpar
								ELSE tot_mZ.idpar
							END idpar0_tot_mZ,
							CASE
								WHEN tot_mZ.idpar IS NULL AND tot_mZbis.idpar IS NOT NULL THEN tot_mZbis.dcntpa
								ELSE tot_mZ.dcntpa
							END area0_tot_mZ,
							CASE
								WHEN tot_mZ.idpar IS NULL AND tot_mZbis.idpar IS NOT NULL THEN tot_mZbis.idprocpte
								ELSE tot_mZ.idprocpte
							END idprocpte0_tot_mZ,
							'saut de millésime sans évolution' type_evol
						FROM anaseq.mX_complexe comp
						LEFT JOIN %I.%I tot_mZ ON comp.idpar0 = tot_mZ.idpar---on check si on retrouve une parcelle en mZ
						LEFT JOIN %I.%I tot_mZbis ON comp.idpar0= tot_mZbis.idpar_inverse),---on regarde si on retrouve les idparfille dans les parcelles de mZ ou non
					
					----+niveau1
					sub2 AS (
						SELECT sub1.iddep22,
							sub1.idcom22,
							sub1.idfil_mX,
							sub1.idpar0,
							sub1.geom0,
							sub1.area0,
							sub1.idpar1,
							sub1.idpar2,
							sub1.idpar3,
							sub1.idpar0_tot_mZ,
							sub1.area0_tot_mZ,
							sub1.idprocpte0_tot_mZ,
							CASE
								WHEN tot_mZ.idpar IS NULL AND tot_mZbis.idpar IS NOT NULL THEN tot_mZbis.idpar
								ELSE tot_mZ.idpar
							END idpar1_tot_mZ,
							CASE
								WHEN tot_mZ.idpar IS NULL AND tot_mZbis.idpar IS NOT NULL THEN tot_mZbis.dcntpa
								ELSE tot_mZ.dcntpa
							END area1_tot_mZ,
							CASE
								WHEN tot_mZ.idpar IS NULL AND tot_mZbis.idpar IS NOT NULL THEN tot_mZbis.idprocpte
								ELSE tot_mZ.idprocpte
							END idprocpte1_tot_mZ,
							'évolution simple avec saut de millésime' type_evol
						FROM sub1
						LEFT JOIN %I.%I tot_mZ ON sub1.idpar1 = tot_mZ.idpar---on check si on retrouve une parcelle en mZ
							AND sub1.idpar0_tot_mZ IS NULL---seulement si la géométrie n'a pas déjà été retrouvée
						LEFT JOIN %I.%I tot_mZbis ON sub1.idpar1= tot_mZbis.idpar_inverse---on regarde si on retrouve les idparfille dans les parcelles de mZ ou non
							AND sub1.idpar0_tot_mZ IS NULL),
					----+niveau2
					sub3 AS (
						SELECT sub2.iddep22,
							sub2.idcom22,
							sub2.idfil_mX,
							sub2.idpar0,
							sub2.geom0,
							sub2.area0,
							sub2.idpar1,
							sub2.idpar2,
							sub2.idpar3,
							sub2.idpar0_tot_mZ,
							sub2.area0_tot_mZ,
							sub2.idprocpte0_tot_mZ,
							sub2.idpar1_tot_mZ,
							sub2.area1_tot_mZ,
							sub2.idprocpte1_tot_mZ,
							CASE
								WHEN tot_mZ.idpar IS NULL AND tot_mZbis.idpar IS NOT NULL THEN tot_mZbis.idpar
								ELSE tot_mZ.idpar
							END idpar2_tot_mZ,
							CASE
								WHEN tot_mZ.idpar IS NULL AND tot_mZbis.idpar IS NOT NULL THEN tot_mZbis.dcntpa
								ELSE tot_mZ.dcntpa
							END area2_tot_mZ,
							CASE
								WHEN tot_mZ.idpar IS NULL AND tot_mZbis.idpar IS NOT NULL THEN tot_mZbis.idprocpte
								ELSE tot_mZ.idprocpte
							END idprocpte2_tot_mZ,
							'évolution complexe avec saut de millésime' type_evol
						FROM sub2
						LEFT JOIN %I.%I tot_mZ ON sub2.idpar2 = tot_mZ.idpar---on check si on retrouve une parcelle en mZ
							AND sub2.idpar1_tot_mZ IS NULL
						LEFT JOIN %I.%I tot_mZbis ON sub2.idpar2= tot_mZbis.idpar_inverse---on regarde si on retrouve les idparfille dans les parcelles de mZ ou non
							AND sub2.idpar1_tot_mZ IS NULL)
					
					----+niveau3
					SELECT sub3.iddep22,
						sub3.idcom22,
						sub3.idfil_mX,
						sub3.idpar0,
						sub3.geom0,
						sub3.area0,
						sub3.idpar1,
						sub3.idpar2,
						sub3.idpar3,
						sub3.idpar0_tot_mZ,
						sub3.area0_tot_mZ,
						sub3.idprocpte0_tot_mZ,
						sub3.idpar1_tot_mZ,
						sub3.area1_tot_mZ,
						sub3.idprocpte1_tot_mZ,
						sub3.idpar2_tot_mZ,
						sub3.area2_tot_mZ,
						sub3.idprocpte2_tot_mZ,
						CASE
							WHEN tot_mZ.idpar IS NULL AND tot_mZbis.idpar IS NOT NULL THEN tot_mZbis.idpar
							ELSE tot_mZ.idpar
						END idpar3_tot_mZ,
						CASE
							WHEN tot_mZ.idpar IS NULL AND tot_mZbis.idpar IS NOT NULL THEN tot_mZbis.dcntpa
							ELSE tot_mZ.dcntpa
						END area3_tot_mZ,
						CASE
							WHEN tot_mZ.idpar IS NULL AND tot_mZbis.idpar IS NOT NULL THEN tot_mZbis.idprocpte
							ELSE tot_mZ.idprocpte
						END idprocpte3_tot_mZ,
						'évolution complexe avec saut de millésime' type_evol
					FROM sub3
					LEFT JOIN %I.%I tot_mZ ON sub3.idpar3 = tot_mZ.idpar---on check si on retrouve une parcelle en mZ
						AND sub3.idpar2_tot_mZ IS NULL
					LEFT JOIN %I.%I tot_mZbis ON sub3.idpar3= tot_mZbis.idpar_inverse---on regarde si on retrouve les idparfille dans les parcelles de mZ ou non
						AND sub3.idpar2_tot_mZ IS NULL
					);
				DROP TABLE anaseq.mX_complexe CASCADE;
				\$\$,
				schema_parcelles_dep_mZ, table_parcelles_dep_mZ,
				schema_parcelles_dep_mZ, table_parcelles_dep_mZ,
				schema_parcelles_dep_mZ, table_parcelles_dep_mZ,
				schema_parcelles_dep_mZ, table_parcelles_dep_mZ,
				schema_parcelles_dep_mZ, table_parcelles_dep_mZ,
				schema_parcelles_dep_mZ, table_parcelles_dep_mZ,
				schema_parcelles_dep_mZ, table_parcelles_dep_mZ,
				schema_parcelles_dep_mZ, table_parcelles_dep_mZ
				);
			COMMIT;

			---Extraction et élagage des filiations complexes qu'on retrouve dans les Fichiers Fonciers
			EXECUTE format(
				\$\$
				DROP TABLE IF EXISTS anaseq.mx_sauts_millesimes_ff CASCADE;
				CREATE TABLE anaseq.mx_sauts_millesimes_ff AS (
					SELECT iddep22,
						idcom22,
						idfil_mX,
						idpar0,
						geom0,
						area0,
						CASE
							WHEN idpar3_tot_mZ IS NOT NULL THEN idpar3_tot_mZ
							WHEN idpar2_tot_mZ IS NOT NULL THEN idpar2_tot_mZ
							WHEN idpar1_tot_mZ IS NOT NULL THEN idpar1_tot_mZ
							ELSE NULL
						END idpar_new,
						CASE
							WHEN idpar3_tot_mZ IS NOT NULL THEN idprocpte3_tot_mZ
							WHEN idpar2_tot_mZ IS NOT NULL THEN idprocpte2_tot_mZ
							WHEN idpar1_tot_mZ IS NOT NULL THEN idprocpte1_tot_mZ
							ELSE NULL
						END idprocpte_new,
						CASE
							WHEN idpar3_tot_mZ IS NOT NULL THEN area3_tot_mZ
							WHEN idpar2_tot_mZ IS NOT NULL THEN area2_tot_mZ
							WHEN idpar1_tot_mZ IS NOT NULL THEN area1_tot_mZ
							ELSE NULL
						END area_new,
						type_evol
					FROM anaseq.mx_sauts_millesimes
					WHERE idpar1_tot_mZ IS NOT NULL
						OR idpar2_tot_mZ IS NOT NULL
						OR idpar3_tot_mZ IS NOT NULL);
				\$\$);
			COMMIT;

			---Création de la table de passage temporaire mX_mZ
			EXECUTE format(
				\$\$
				DROP TABLE IF EXISTS anaseq.passage_mX_mZ CASCADE;
				CREATE TABLE anaseq.passage_mX_mZ AS (

					SELECT iddep22,
						idcom22,
						idfil_mX,
						idpar0 mX_idpar,
						geom0 mX_geom,
						area0 mX_area,
						idpar_new mZ_idpar,
						area_new mZ_area,
						idprocpte_new mZ_idprocpte,
						type_evol
					FROM anaseq.mx_sauts_millesimes_ff
					GROUP BY iddep22,
						idcom22,
						idfil_mX,
						mX_idpar,
						mX_geom,
						mX_area,
						mZ_idpar,
						mZ_area,
						mZ_idprocpte,
						type_evol);
					
				CREATE INDEX sub_mZ_idpar_idx ON anaseq.passage_mX_mZ(mZ_idpar);
				\$\$);
			COMMIT;

			---Recherche des dernières géométries parcellaires dans la table de passage mX_mZ
			EXECUTE format(
				\$\$
				---Création des colonnes et index
				ALTER TABLE anaseq.passage_mX_mZ DROP COLUMN IF EXISTS mZ_lastgeompar CASCADE;
				ALTER TABLE anaseq.passage_mX_mZ ADD COLUMN mZ_lastgeompar geometry;
				CREATE INDEX mX_mZ_passage_geom_mX_idx ON anaseq.passage_mX_mZ USING gist(mX_geom);
				CREATE INDEX mX_mZ_passage_geom_mZ_idx ON anaseq.passage_mX_mZ USING gist(mZ_lastgeompar);
				CREATE INDEX mX_mZ_passage_idpar_new_idx ON anaseq.passage_mX_mZ(mZ_idpar);
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

				IF millesime_de_reference::numeric BETWEEN mZ::numeric-1 AND mX::numeric THEN---On ne recherche lastgeompar que 2 ans avant et 1 an après l'année considérée (sinon on l'aurait déjà en cas simple)
			
					EXECUTE format(
						\$\$		
						UPDATE anaseq.passage_mX_mZ a
						SET mZ_lastgeompar = ST_MakeValid(b.geompar)
						FROM %I.%I b
						WHERE a.mZ_lastgeompar IS NULL
							AND b.geompar IS NOT NULL
							AND a.mZ_idpar = b.idpar
							AND b.vecteur LIKE 'V'; --- uniquement les géométries vectorisées
		
						UPDATE anaseq.passage_mX_mZ a
						SET mZ_lastgeompar = ST_MakeValid(b.geompar)
						FROM %I.%I b
						WHERE a.mZ_lastgeompar IS NULL
							AND b.geompar IS NOT NULL
							AND a.mZ_idpar = b.idpar_inverse---si changement communal
							AND b.vecteur LIKE 'V'; --- uniquement les géométries vectorisées
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
					%I,---idfil_mX
					%I,---idpar_mX
					%I,---idpar_mY
					%I,---idpar_mZ
					%I,---idprocpte_mZ
					geom,
					type_evol
					)
				SELECT %L,
					iddep22,
					idcom22,
					%L,---année d'évolution initiale
					idfil_mX,
					mX_idpar,
					'saut',
					mZ_idpar,
					mZ_idprocpte,
					ST_Union(ST_CollectionExtract(ST_MakeValid(ST_Intersection(mX_geom,mZ_lastgeompar)),3)),
					type_evol
				FROM anaseq.passage_mX_mZ
				WHERE mX_geom IS NOT NULL
					AND mZ_lastgeompar IS NOT NULL
				GROUP BY iddep22,
					idcom22,
					idfil_mX,
					mX_idpar,
					mZ_idpar,
					mZ_idprocpte,
					type_evol;
				
				UPDATE anaseq.%I
				SET area = ST_Area(geom)
				WHERE area IS NULL
					AND annee = %L
					AND %I LIKE 'saut'
					AND geom IS NOT NULL;
				\$\$,
				table_filiation_mY,
				idfil_filiation_mX,
				mX_idpar,
				mY_idpar,
				mZ_idpar,
				mZ_idprocpte,
				dep,
				m1,
				table_filiation_mY,
				m1,
				mY_idpar);
			COMMIT;


			---Insert des non-superpositions géométriques dans la table nodfi
			EXECUTE format(
			\$\$
			DROP TABLE IF EXISTS anaseq.fil_mY_uni CASCADE;
			CREATE TABLE anaseq.fil_mY_uni AS (
				SELECT ST_Union(ST_MakeValid(ST_CollectionExtract(geom,3))) geom
				FROM anaseq.%I
				WHERE annee = %L
					AND type_evol LIKE '%%saut%%'
			);
			CREATE INDEX fil_mY_uni_geom_idx ON anaseq.fil_mY_uni USING gist(geom);
			
			DROP TABLE IF EXISTS anaseq.passuni CASCADE;
			CREATE TABLE anaseq.passuni AS (
				SELECT ST_Union(ST_MakeValid(ST_CollectionExtract(mX_geom,3))) geom
				FROM anaseq.passage_mX_mZ
				WHERE mX_geom IS NOT NULL
					AND mZ_lastgeompar IS NOT NULL
			);
			CREATE INDEX passuni_geom_idx ON anaseq.passuni USING gist(geom);
			
			DROP TABLE IF EXISTS anaseq.diff CASCADE;
			\$\$,
			table_filiation_mY,
			m1
			);
			
			BEGIN
				CREATE TABLE anaseq.diff AS (
					SELECT ST_MakeValid(ST_CollectionExtract(ST_Difference(passuni.geom, fil_mY_uni.geom),3)) geom
					FROM anaseq.fil_mY_uni, anaseq.passuni	
				);
				CREATE INDEX diff_geom_idx ON anaseq.diff USING gist(geom);

			EXCEPTION----En cas d'erreur de différence spatiale, on refait avec ST_MemUnion sur la table anaseq.fil_mY_uni
				WHEN OTHERS THEN
					
					EXECUTE format(
					\$\$
					DROP TABLE IF EXISTS anaseq.fil_mY_uni CASCADE;
					CREATE TABLE anaseq.fil_mY_uni AS (
						SELECT ST_MemUnion(ST_MakeValid(ST_CollectionExtract(geom,3))) geom
						FROM anaseq.%I
						WHERE annee = %L
							AND type_evol LIKE '%%saut%%'
					);
					CREATE INDEX fil_mY_uni_geom_idx ON anaseq.fil_mY_uni USING gist(geom);
					
					DROP TABLE IF EXISTS anaseq.diff CASCADE;
					CREATE TABLE anaseq.diff AS (
						SELECT ST_MakeValid(ST_CollectionExtract(ST_Difference(passuni.geom, fil_mY_uni.geom),3)) geom
						FROM anaseq.fil_mY_uni, anaseq.passuni	
					);
					CREATE INDEX diff_geom_idx ON anaseq.diff USING gist(geom);
					\$\$,
					table_filiation_mY,
					m1);

			END;
			

			EXECUTE format(
				\$\$	
				INSERT INTO anaseq.reste_mX_nodfi(
					iddep22,
					idcom22,
					idfil_mX,
					mX_idpar,
					mX_geom)
				SELECT iddep22,
					idcom22,
					idfil_mX,
					mX_idpar,
					ST_Union(ST_Difference(mX_geom,mZ_lastgeompar))
				FROM anaseq.passage_mX_mZ---table de passage préétablie
				WHERE mX_geom IS NOT NULL
					AND mZ_lastgeompar IS NOT NULL
				GROUP BY iddep22,
					idcom22,
					idfil_mX,
					mX_idpar;
				
				UPDATE anaseq.reste_mX_nodfi
				SET mX_area = ST_Area(mX_geom)
				WHERE mX_area IS NULL
					AND mX_geom IS NOT NULL;

				DROP TABLE anaseq.fil_mY_uni CASCADE;
				DROP TABLE anaseq.passuni CASCADE;
				DROP TABLE anaseq.diff CASCADE;
				\$\$);
			COMMIT;

			---On compte pour chaque parcelle en mX la surface non-retrouvée géométriquement
			---> pour distibuter équitablement la surface restante entre les parcelles filles/mères
			EXECUTE format(
				\$\$
				ALTER TABLE anaseq.passage_mX_mZ ADD COLUMN surf_geom_manquante numeric;

				WITH sub AS(
					SELECT %I,
						SUM(area) area_retrouvee
					FROM anaseq.%I
					WHERE annee = %L
						AND %I LIKE 'saut'
					GROUP BY %I
				)
				UPDATE anaseq.passage_mX_mZ a
				SET surf_geom_manquante = COALESCE(COALESCE(a.mX_area,0)-COALESCE(sub.area_retrouvee,0),0)
				FROM sub
				WHERE a.mX_idpar = sub.%I;

				UPDATE anaseq.passage_mX_mZ
				SET surf_geom_manquante = mX_area
				WHERE surf_geom_manquante IS NULL
				\$\$,
				mX_idpar,
				table_filiation_mY,
				m1,
				mY_idpar,
				mX_idpar,
				mX_idpar);
			COMMIT;

			---Table des parcelles avec lacune géométrique
			EXECUTE format(
				\$\$
				DROP TABLE IF EXISTS anaseq.passage_mX_mZ_lacunesgeom CASCADE;
				CREATE TABLE anaseq.passage_mX_mZ_lacunesgeom AS (
					SELECT * FROM anaseq.passage_mX_mZ
					WHERE mX_geom IS NULL
						OR mZ_lastgeompar IS NULL);
				\$\$);
			COMMIT;

			--Calcul du nombre de filles et de mères
			EXECUTE format(
				\$\$
				ALTER TABLE anaseq.passage_mX_mZ_lacunesgeom ADD COLUMN nb_meres int;
				ALTER TABLE anaseq.passage_mX_mZ_lacunesgeom ADD COLUMN nb_filles int;

				WITH sub AS (
					SELECT mX_idpar, COUNT(DISTINCT mZ_idpar) nb_meres
					FROM anaseq.passage_mX_mZ_lacunesgeom
					GROUP BY mX_idpar)
				UPDATE anaseq.passage_mX_mZ_lacunesgeom a
				SET nb_meres = sub.nb_meres
				FROM sub
				WHERE a.mX_idpar = sub.mX_idpar;

				WITH sub AS (
					SELECT mZ_idpar, COUNT(DISTINCT mX_idpar) nb_filles
					FROM anaseq.passage_mX_mZ_lacunesgeom
					GROUP BY mZ_idpar			
				)
				UPDATE anaseq.passage_mX_mZ_lacunesgeom a
				SET nb_filles = sub.nb_filles
				FROM sub
				WHERE a.mZ_idpar = sub.mZ_idpar;
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
					%I,---idfil_mX
					%I,---idpar_mX
					%I,---idpar_mY
					%I,---idpar_mZ
					%I,---idprocpte_mZ
					geom,
					area,
					type_evol
					)
				SELECT %L,
					iddep22,
					idcom22,
					%L,---année d'évolution initiale
					idfil_mX,
					mX_idpar,
					'saut',
					mZ_idpar,
					mZ_idprocpte,
					CASE
						WHEN type_evol LIKE 'division'
							OR type_evol LIKE 'transfert'
							OR (nb_meres = 1 AND nb_filles >= 1)
								THEN mX_geom
						ELSE NULL::geometry
					END,
					CASE
						WHEN type_evol LIKE 'division'
							OR type_evol LIKE 'transfert'
							OR (nb_meres = 1 AND nb_filles >= 1)
								THEN mX_area
						ELSE surf_geom_manquante/(nb_meres+nb_filles-1)---divise la surface par le nombre de portions
					END,
					CONCAT(type_evol, ' (avec géométrie invalide)')
				FROM anaseq.passage_mX_mZ_lacunesgeom sub
				WHERE mX_geom IS NULL
					OR mZ_lastgeompar IS NULL
				GROUP BY iddep22,
					idcom22,
					idfil_mX,
					mX_idpar,
					mZ_idpar,
					mZ_idprocpte,
					nb_meres,
					nb_filles,
					mX_geom,
					mX_area,
					surf_geom_manquante,
					type_evol;
				\$\$,
				table_filiation_mY,
				idfil_filiation_mX,
				mX_idpar,
				mY_idpar,
				mZ_idpar,
				mZ_idprocpte,
				dep,
				m1);
			COMMIT;

			---Suppression des parcelles retrouvées
			---et insert dans la table des non-DFI des parcelles non-retrouvées
			EXECUTE format(
				\$\$
				DELETE FROM anaseq.mX_sauts_millesimes saut
				USING anaseq.mX_sauts_millesimes_ff ff
				WHERE saut.idpar0 = ff.idpar0
					AND ff.idpar_new IS NOT NULL;
				
				DROP TABLE anaseq.mX_sauts_millesimes_ff CASCADE;
				DROP TABLE anaseq.passage_mX_mZ CASCADE;
				DROP TABLE anaseq.passage_mX_mZ_lacunesgeom CASCADE;
				
				INSERT INTO anaseq.reste_mx_nodfi
					SELECT iddep22,
						idcom22,
						idfil_mX,
						idpar0,
						geom0,
						area0
					FROM anaseq.mX_sauts_millesimes;
				DROP TABLE anaseq.mX_sauts_millesimes CASCADE;
				\$\$);
			COMMIT;
		END IF;



		-------------------------------------------
		---Approche géométrique pour les parcelles sans DFI dont les espaces non cadastrés
		-------------------------------------------
		RAISE NOTICE '-------BRANCHE 2 (approche géométrique)--------';
		
		FOR idcom22 IN EXECUTE 'SELECT LEFT(idcom22,3) FROM anaseq.reste_mX_nodfi GROUP BY LEFT(idcom22,3) ORDER BY LEFT(idcom22,3)'---On boucle sur des groupes de communes pour alléger la mémoire vive
		LOOP
		
			RAISE NOTICE 'Communes %',idcom22;
		
			---Création des géométries des parcelles à filier
			EXECUTE format(
				\$\$
				DROP TABLE IF EXISTS anaseq.reste_mX_nodfi_com CASCADE;
				CREATE TABLE anaseq.reste_mX_nodfi_com AS (
					SELECT * FROM anaseq.reste_mX_nodfi WHERE LEFT(idcom22,3) LIKE %L);				
					
				ALTER TABLE anaseq.reste_mX_nodfi_com DROP COLUMN IF EXISTS mX_lastgeompoint CASCADE;
				ALTER TABLE anaseq.reste_mX_nodfi_com DROP COLUMN IF EXISTS mX_lastgeompoint_buffer200 CASCADE;
				ALTER TABLE anaseq.reste_mX_nodfi_com ADD COLUMN mX_lastgeompoint geometry;
				ALTER TABLE anaseq.reste_mX_nodfi_com ADD COLUMN mX_lastgeompoint_buffer200 geometry;
				CREATE INDEX IF NOT EXISTS reste_mX_nodfi_mX_lastgeompoint_idx ON anaseq.reste_mX_nodfi_com USING gist(mX_lastgeompoint);
				CREATE INDEX IF NOT EXISTS reste_mX_nodfi_mX_lastgeompoint_buffer200_idx ON anaseq.reste_mX_nodfi_com USING gist(mX_lastgeompoint_buffer200);
				CREATE INDEX IF NOT EXISTS reste_mX_nodfi_mX_geom ON anaseq.reste_mX_nodfi_com USING gist(mX_geom);
				CREATE INDEX IF NOT EXISTS reste_mX_nodfi_mX_idpar_idx ON anaseq.reste_mX_nodfi_com(mX_idpar);
				\$\$,
				idcom22);
			COMMIT;
			
			---Recherche du centroïde des parcelles ou portions de parcelles	
			EXECUTE format(
				\$\$
				DELETE FROM anaseq.reste_mX_nodfi_com
				WHERE mX_geom IS NOT NULL
					AND mX_area = 0;
				
				UPDATE anaseq.reste_mX_nodfi_com a
				SET mX_lastgeompoint = ST_PointOnSurface(mX_geom);
		
				UPDATE anaseq.reste_mX_nodfi_com
				SET mX_lastgeompoint_buffer200 = ST_Buffer(mX_lastgeompoint,200)---pour rechercher les parcelles qui l'intersectent
				WHERE mX_lastgeompoint IS NOT NULL;
				\$\$);
			COMMIT;
			
			---Sélection de toutes les sections de parcelles en mY
			---qui intersectent le buffer de 200m (ce qui permet de prendre les parcelles avec lacune géométrique pour essayer de la corriger)
			EXECUTE format(
				\$\$
				DROP TABLE IF EXISTS anaseq.par_mY CASCADE;
				CREATE TABLE anaseq.par_mY AS (
					WITH liste_sections AS (
						SELECT tot_mY.ccodep,
							tot_mY.idcom,
							tot_mY.idsec
						FROM anaseq.reste_mX_nodfi_com nodfi
						JOIN %I.%I tot_mY
							ON ST_Intersects(nodfi.mX_lastgeompoint_buffer200, tot_mY.geompar)
						GROUP BY tot_mY.ccodep,
							tot_mY.idcom,
							tot_mY.idsec)
					
					SELECT tot_mY.ccodep,
						tot_mY.idcom,
						tot_mY.idpar,
						tot_mY.idprocpte
					FROM %I.%I tot_mY
					JOIN liste_sections sec ON tot_mY.idsec = sec.idsec
				);
				\$\$,
				schema_parcelles_dep_mY, table_parcelles_dep_mY,
				schema_parcelles_dep_mY, table_parcelles_dep_mY);
			COMMIT;

			---Recherche de la dernière géométrie en par_mY
			EXECUTE format(
				\$\$
				ALTER TABLE anaseq.par_mY DROP COLUMN IF EXISTS lastgeompar CASCADE;
				ALTER TABLE anaseq.par_mY ADD COLUMN lastgeompar geometry;
				CREATE INDEX par_mY_lastgeompar_idx ON anaseq.par_mY USING gist(lastgeompar);
				CREATE INDEX par_mY_idpar ON anaseq.par_mY(idpar);
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

				IF millesime_de_reference::numeric BETWEEN mZ::numeric-1 AND mX::numeric THEN---On ne recherche lastgeompar que 2 ans avant et 1 an après l'année considérée (sinon on l'aurait déjà en cas simple)
				
					EXECUTE format(
						\$\$
						UPDATE anaseq.par_mY a
						SET lastgeompar = ST_MakeValid(b.geompar)
						FROM %I.%I b
						WHERE a.lastgeompar IS NULL
							AND b.geompar IS NOT NULL
							AND a.idpar = b.idpar
							AND b.vecteur LIKE 'V'; --- uniquement les géométries vectorisées
		
						UPDATE anaseq.par_mY a
						SET lastgeompar = ST_MakeValid(b.geompar)
						FROM %I.%I b
						WHERE a.lastgeompar IS NULL
							AND b.geompar IS NOT NULL
							AND a.idpar = b.idpar_inverse
							AND b.vecteur LIKE 'V'; --- uniquement les géométries vectorisées
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
						nodfi.idfil_mX,
						ST_Union(ST_CollectionExtract(ST_MakeValid(ST_Intersection(nodfi.mX_geom,par_mY.lastgeompar)),3)) geom,
						nodfi.mX_idpar,---parcelle en mX
						par_mY.idpar mY_idpar,---parcelle en mY
						par_mY.idprocpte mY_idprocpte
					FROM anaseq.reste_mX_nodfi_com nodfi
					JOIN anaseq.par_mY par_mY ON ST_Intersects(nodfi.mX_geom,par_mY.lastgeompar)
					GROUP BY nodfi.iddep22,
						nodfi.idcom22,---commune concernée
						nodfi.idfil_mX,
						nodfi.mX_idpar,
						par_mY.idpar,
						par_mY.idprocpte)
							
				INSERT INTO anaseq.%I(
					ccodep,
					iddep22,
					idcom22,
					annee,
					%I,---idfil_mX
					%I,---idpar_mX
					%I,---idpar_mY
					%I,---idprocpte_mY
					%I,---idpar_mZ
					geom,
					type_evol
					)
				SELECT %L,
					iddep22,
					idcom22,
					%L,---annee d'évolution initiale
					idfil_mX,
					mX_idpar,
					mY_idpar,
					mY_idprocpte,
					'SO',---sans objet en mZ
					geom,
					'filiation géométrique'
				FROM sub
				GROUP BY iddep22,
					idcom22,---commune concernée
					idfil_mX,
					mX_idpar,
					mY_idpar,
					mY_idprocpte,
					geom
				ON CONFLICT DO NOTHING;---évite les duplicats en cas d'erreur de géométrie
				
				UPDATE anaseq.%I SET area = ST_Area(geom)
				WHERE LEFT(idcom22,3) LIKE %L
					AND type_evol LIKE 'filiation géométrique';
				\$\$,
				table_filiation_mY,
				idfil_filiation_mX,
				mX_idpar,
				mY_idpar,
				mY_idprocpte,
				mZ_idpar,
				dep,
				m1,
				table_filiation_mY,
				idcom22);
			COMMIT;

			---Création de la table des géométries parcellaires non retrouvées à ce stade
			EXECUTE format(
				\$\$
				DROP TABLE IF EXISTS anaseq.mx_nodfi_uni CASCADE;
				CREATE TABLE anaseq.mx_nodfi_uni AS (
					SELECT ST_Union(ST_MakeValid(ST_CollectionExtract(mX_geom,3))) geom
					FROM anaseq.reste_mX_nodfi_com);
				CREATE INDEX nodfi_uni_geom_idx ON anaseq.mx_nodfi_uni USING gist(geom);
				
				DROP TABLE IF EXISTS anaseq.mx_dfi_retrouve_uni CASCADE;
				CREATE TABLE anaseq.mx_dfi_retrouve_uni AS (
					SELECT ST_Union(ST_MakeValid(ST_CollectionExtract(geom,3))) geom
					FROM anaseq.%I
					WHERE annee = %L
						AND LEFT(idcom22,3) LIKE %L
						AND type_evol LIKE 'filiation géométrique');
				CREATE INDEX dfi_retrouve_uni_geom_idx ON anaseq.mx_dfi_retrouve_uni USING gist(geom);
				
				DROP TABLE IF EXISTS anaseq.mx_diff CASCADE;
				\$\$,
				table_filiation_mY,
				m1,
				idcom22
				);
				
			BEGIN
				CREATE TABLE anaseq.mx_diff AS (
					SELECT ST_Union(ST_CollectionExtract(ST_MakeValid(ST_Difference(nodfi.geom,dfi_retrouve_uni.geom)),3)) geom
					FROM anaseq.mx_nodfi_uni nodfi, anaseq.mx_dfi_retrouve_uni dfi_retrouve_uni);
				CREATE INDEX diff_geom_idx ON anaseq.mx_diff USING gist(geom);

			EXCEPTION----En cas d'erreur de différence spatiale, on refait avec ST_MemUnion
				WHEN OTHERS THEN
					
					EXECUTE format(
					\$\$
					DROP TABLE IF EXISTS anaseq.mx_nodfi_uni CASCADE;
					CREATE TABLE anaseq.mx_nodfi_uni AS (
						SELECT ST_MemUnion(ST_MakeValid(ST_CollectionExtract(mX_geom,3))) geom
						FROM anaseq.reste_mX_nodfi_com);
					CREATE INDEX nodfi_uni_geom_idx ON anaseq.mx_nodfi_uni USING gist(geom);
					
					DROP TABLE IF EXISTS anaseq.mx_dfi_retrouve_uni CASCADE;
					CREATE TABLE anaseq.mx_dfi_retrouve_uni AS (
						SELECT ST_MemUnion(ST_MakeValid(ST_CollectionExtract(geom,3))) geom
						FROM anaseq.%I
						WHERE annee = %L
							AND LEFT(idcom22,3) LIKE %L
							AND type_evol LIKE 'filiation géométrique');
					CREATE INDEX dfi_retrouve_uni_geom_idx ON anaseq.mx_dfi_retrouve_uni USING gist(geom);
					
					CREATE TABLE anaseq.mx_diff AS (
						SELECT ST_Union(ST_CollectionExtract(ST_MakeValid(ST_Difference(nodfi.geom,dfi_retrouve_uni.geom)),3)) geom
						FROM anaseq.mx_nodfi_uni nodfi, anaseq.mx_dfi_retrouve_uni dfi_retrouve_uni);
					CREATE INDEX diff_geom_idx ON anaseq.mx_diff USING gist(geom);
					\$\$,
					table_filiation_mY,
					m1,
					idcom22);

			END;
			
			EXECUTE format(
				\$\$
				DROP TABLE IF EXISTS anaseq.mx_nodfi_etape2 CASCADE;
				CREATE TABLE anaseq.mx_nodfi_etape2 AS (
					SELECT nodfi.iddep22,
						nodfi.idcom22,
						nodfi.idfil_mX,
						nodfi.mX_idpar,
						ST_Union(ST_CollectionExtract(ST_MakeValid(ST_Intersection(nodfi.mX_geom,diff.geom)),3)) geom
					FROM anaseq.reste_mX_nodfi_com nodfi
					JOIN anaseq.mx_diff diff ON ST_Intersects(nodfi.mX_geom,diff.geom)
					GROUP BY nodfi.iddep22,
						nodfi.idcom22,
						nodfi.idfil_mX,
						nodfi.mX_idpar
				);
							
				DROP TABLE anaseq.mx_nodfi_uni CASCADE;
				DROP TABLE anaseq.mx_dfi_retrouve_uni CASCADE;
				DROP TABLE anaseq.mx_diff CASCADE;
				DROP TABLE anaseq.reste_mX_nodfi_com CASCADE;
				DROP TABLE anaseq.par_mY CASCADE;
				\$\$);
			COMMIT;

			---Insert des maintiens en NC
			EXECUTE format(
				\$\$
				INSERT INTO anaseq.%I(
					ccodep,
					iddep22,
					idcom22,
					annee,
					%I,---idfil_mX
					%I,---idpar_mX
					%I,---idpar_mY
					%I,---idprocpte_mY
					%I,---idpar_mZ
					geom,
					area,
					type_evol
					)
				SELECT %L,
					iddep22,
					idcom22,
					%L,---annee d'évolution initiale
					idfil_mX,
					mX_idpar,
					'NC',---mY_idpar
					'NC',---mY_idprocpte
					'SO',---sans objet en mZ
					geom,
					ST_Area(geom),
					'maintien en NC'
				FROM anaseq.mx_nodfi_etape2
				WHERE mX_idpar LIKE 'NC'
				GROUP BY iddep22,
					idcom22,---commune concernée
					idfil_mX,
					mX_idpar,
					geom
				ON CONFLICT DO NOTHING;---évite les duplicats en cas d'erreur de géométrie

				DELETE FROM anaseq.mx_nodfi_etape2 WHERE mX_idpar LIKE 'NC';
				\$\$,
				table_filiation_mY,
				idfil_filiation_mX,
				mX_idpar,
				mY_idpar,
				mY_idprocpte,
				mZ_idpar,
				dep,
				m1);
			COMMIT;


			------------------------------------------
			---Approche géométrique avec saut de millésime pour les parcelles restantes
			-------------------------------------------
			
			IF X < 5 THEN---Pas de saut de millésime vers 2010 si m1 = 2016
					
				RAISE NOTICE 'APPROCHE GEOMETRIQUE POUR SAUTS DE MILLESIME';

				---Création des centroïdes et buffer sur les parcelles restantes
				EXECUTE format(
					\$\$
					ALTER TABLE anaseq.mx_nodfi_etape2 DROP COLUMN IF EXISTS lastgeompoint;
					ALTER TABLE anaseq.mx_nodfi_etape2 DROP COLUMN IF EXISTS lastgeompoint_buffer200;
					ALTER TABLE anaseq.mx_nodfi_etape2 ADD COLUMN lastgeompoint geometry;
					ALTER TABLE anaseq.mx_nodfi_etape2 ADD COLUMN lastgeompoint_buffer200 geometry;
					CREATE INDEX mx_nodfi_etape2_lastgeompoint_buffer200_idx ON anaseq.mx_nodfi_etape2 USING gist(lastgeompoint_buffer200);
					
					UPDATE anaseq.mx_nodfi_etape2
					SET lastgeompoint = ST_PointOnSurface(geom);
					UPDATE anaseq.mx_nodfi_etape2
					SET lastgeompoint_buffer200 = ST_Buffer(lastgeompoint,200);
					\$\$);
				COMMIT;

				---Sélection de toutes les sections de parcelles en mZ
				---qui intersectent le buffer de 200m (ce qui permet de prendre les parcelles avec lacune géométrique pour essayer de la corriger)
				EXECUTE format(
					\$\$
					DROP TABLE IF EXISTS anaseq.par_mZ CASCADE;
					CREATE TABLE anaseq.par_mZ AS (
						WITH liste_sections AS (
							SELECT tot_mZ.ccodep,
								tot_mZ.idcom,
								tot_mZ.idsec
							FROM anaseq.mX_nodfi_etape2 nodfi
							JOIN %I.%I tot_mZ
								ON ST_Intersects(nodfi.lastgeompoint_buffer200, tot_mZ.geompar)
							GROUP BY tot_mZ.ccodep,
								tot_mZ.idcom,
								tot_mZ.idsec)
						
						SELECT tot_mZ.ccodep,
							tot_mZ.idcom,
							tot_mZ.idpar,
							tot_mZ.idprocpte
						FROM %I.%I tot_mZ
						JOIN liste_sections sec ON tot_mZ.idsec = sec.idsec
					);
					\$\$,
					schema_parcelles_dep_mZ, table_parcelles_dep_mZ,
					schema_parcelles_dep_mZ, table_parcelles_dep_mZ);
				COMMIT;

				---Recherche de la dernière géométrie en par_mZ
				EXECUTE format(
					\$\$
					ALTER TABLE anaseq.par_mZ DROP COLUMN IF EXISTS lastgeompar CASCADE;
					ALTER TABLE anaseq.par_mZ ADD COLUMN lastgeompar geometry;
					CREATE INDEX par_mZ_lastgeompar_idx ON anaseq.par_mZ USING gist(lastgeompar);
					CREATE INDEX par_mZ_idpar ON anaseq.par_mZ(idpar);
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

					IF millesime_de_reference::numeric BETWEEN mZ::numeric-1 AND mX::numeric THEN---On ne recherche lastgeompar que 2 ans avant et 1 an après l'année considérée (sinon on l'aurait déjà en cas simple)

						EXECUTE format(
							\$\$
							UPDATE anaseq.par_mZ a
							SET lastgeompar = ST_MakeValid(b.geompar)
							FROM %I.%I b
							WHERE a.lastgeompar IS NULL
								AND b.geompar IS NOT NULL
								AND a.idpar = b.idpar
								AND b.vecteur LIKE 'V'; --- uniquement les géométries vectorisées
			
							UPDATE anaseq.par_mZ a
							SET lastgeompar = ST_MakeValid(b.geompar)
							FROM %I.%I b
							WHERE a.lastgeompar IS NULL
								AND b.geompar IS NOT NULL
								AND a.idpar = b.idpar_inverse
								AND b.vecteur LIKE 'V'; --- uniquement les géométries vectorisées
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
							nodfi.idfil_mX,
							ST_Union(ST_CollectionExtract(ST_MakeValid(ST_Intersection(nodfi.geom,par_mZ.lastgeompar)),3)) geom,
							nodfi.mX_idpar,---parcelle en mX
							par_mZ.idpar mZ_idpar,---parcelle en mZ
							par_mZ.idprocpte mZ_idprocpte
						FROM anaseq.mx_nodfi_etape2 nodfi
						JOIN anaseq.par_mZ par_mZ ON ST_Intersects(nodfi.geom,par_mZ.lastgeompar)
						GROUP BY nodfi.iddep22,
							nodfi.idcom22,---commune concernée
							nodfi.idfil_mX,
							nodfi.mX_idpar,
							par_mZ.idpar,
							par_mZ.idprocpte)
								
					INSERT INTO anaseq.%I(
						ccodep,
						iddep22,
						idcom22,
						annee,
						%I,---idfil_mX
						%I,---idpar_mX
						%I,---idpar_mY
						%I,---idpar_mZ
						%I,---idprocpte_mZ
						geom,
						type_evol
						)
					SELECT %L,
						iddep22,
						idcom22,
						%L,---annee d'évolution initiale
						idfil_mX,
						mX_idpar,
						'saut',---mY_idpar
						mZ_idpar,
						mZ_idprocpte,
						geom,
						'filiation géométrique avec saut de millésime'
					FROM sub
					GROUP BY iddep22,
						idcom22,---commune concernée
						idfil_mX,
						mX_idpar,
						mZ_idpar,
						mZ_idprocpte,
						geom
					ON CONFLICT DO NOTHING;---évite les duplicats en cas d'erreur de géométrie
					
					UPDATE anaseq.%I SET area = ST_Area(geom) WHERE type_evol LIKE 'filiation géométrique avec saut de millésime';
					\$\$,
					table_filiation_mY,
					idfil_filiation_mX,
					mX_idpar,
					mY_idpar,
					mZ_idpar,
					mZ_idprocpte,
					dep,
					m1,
					table_filiation_mY);
				COMMIT;
			END IF;


			------------------------------------------
			---Passage du reste en non-cadastré
			-------------------------------------------
			RAISE NOTICE 'PASSAGE DU RESTE EN NON-CADASTRE';

			---Création de la table des géométries parcellaires non retrouvées
			EXECUTE format(
				\$\$
				DROP TABLE IF EXISTS anaseq.mx_nodfi_uni CASCADE;
				CREATE TABLE anaseq.mx_nodfi_uni AS (
					SELECT ST_Union(ST_MakeValid(ST_CollectionExtract(geom,3))) geom
					FROM anaseq.mx_nodfi_etape2);
				CREATE INDEX nodfi_uni_geom_idx ON anaseq.mx_nodfi_uni USING gist(geom);
				
				DROP TABLE IF EXISTS anaseq.mx_dfi_retrouve_uni CASCADE;
				CREATE TABLE anaseq.mx_dfi_retrouve_uni AS (
					SELECT ST_Union(ST_MakeValid(ST_CollectionExtract(geom,3))) geom
					FROM anaseq.%I
					WHERE annee = %L
						AND LEFT(idcom22,3) LIKE %L
						AND type_evol LIKE 'filiation géométrique avec saut de millésime');
				CREATE INDEX dfi_retrouve_uni_geom_idx ON anaseq.mx_dfi_retrouve_uni USING gist(geom);
				
				DROP TABLE IF EXISTS anaseq.mx_diff CASCADE;
				\$\$,
				table_filiation_mY,
				m1,
				idcom22
				);			
				
				
			BEGIN
				CREATE TABLE anaseq.mx_diff AS (
					SELECT ST_Union(ST_CollectionExtract(ST_MakeValid(ST_Difference(nodfi.geom,dfi_retrouve_uni.geom)),3)) geom
					FROM anaseq.mx_nodfi_uni nodfi, anaseq.mx_dfi_retrouve_uni dfi_retrouve_uni);
				CREATE INDEX diff_geom_idx ON anaseq.mx_diff USING gist(geom);

			EXCEPTION----En cas d'erreur de différence spatiale, on refait avec ST_MemUnion
				WHEN OTHERS THEN
					
					EXECUTE format(
					\$\$
					DROP TABLE IF EXISTS anaseq.mx_nodfi_uni CASCADE;
					CREATE TABLE anaseq.mx_nodfi_uni AS (
						SELECT ST_MemUnion(ST_MakeValid(ST_CollectionExtract(geom,3))) geom
						FROM anaseq.mx_nodfi_etape2);
					CREATE INDEX nodfi_uni_geom_idx ON anaseq.mx_nodfi_uni USING gist(geom);
					
					DROP TABLE IF EXISTS anaseq.mx_dfi_retrouve_uni CASCADE;
					CREATE TABLE anaseq.mx_dfi_retrouve_uni AS (
						SELECT ST_MemUnion(ST_MakeValid(ST_CollectionExtract(geom,3))) geom
						FROM anaseq.%I
						WHERE annee = %L
							AND LEFT(idcom22) LIKE %L
							AND type_evol LIKE 'filiation géométrique avec saut de millésime');
					CREATE INDEX dfi_retrouve_uni_geom_idx ON anaseq.mx_dfi_retrouve_uni USING gist(geom);
					
					CREATE TABLE anaseq.mx_diff AS (
						SELECT ST_Union(ST_CollectionExtract(ST_MakeValid(ST_Difference(nodfi.geom,dfi_retrouve_uni.geom)),3)) geom
						FROM anaseq.mx_nodfi_uni nodfi, anaseq.mx_dfi_retrouve_uni dfi_retrouve_uni);
					CREATE INDEX diff_geom_idx ON anaseq.mx_diff USING gist(geom);
					\$\$,
					table_filiation_mY,
					m1,
					idcom22);

			END;
				
			EXECUTE format(
				\$\$			
				DROP TABLE IF EXISTS anaseq.mx_non_cadastre CASCADE;
				CREATE TABLE anaseq.mx_non_cadastre AS (
					SELECT nodfi.iddep22,
						nodfi.idcom22,
						nodfi.idfil_mX,
						nodfi.mX_idpar,
						ST_Union(ST_CollectionExtract(ST_MakeValid(ST_Intersection(nodfi.geom,diff.geom)),3)) geom
					FROM anaseq.mx_nodfi_etape2 nodfi
					JOIN anaseq.mx_diff diff ON ST_Intersects(nodfi.geom,diff.geom)
					GROUP BY nodfi.iddep22,
						nodfi.idcom22,
						nodfi.idfil_mX,
						nodfi.mX_idpar
				);
				ALTER TABLE anaseq.mx_non_cadastre ADD COLUMN area numeric;
				UPDATE anaseq.mx_non_cadastre SET area = ST_Area(geom);
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
					%I,---idfil_mX
					%I,---idpar_mX
					%I,---idpar_mY
					%I,---idprocpte_mY
					%I,---idpar_mZ
					geom,
					area,
					type_evol
					)
				SELECT %L,
					iddep22,
					idcom22,
					%L,---annee d'évolution initiale
					idfil_mX,
					mX_idpar,
					'NC',---mY_idpar
					'NC',---mY_idprocpte
					'SO',---mZ_idpar
					geom,
					area,
					'sortie de NC (absence des FF et DFI)'
				FROM anaseq.mx_non_cadastre
				GROUP BY iddep22,
					idcom22,---commune concernée
					idfil_mX,
					mX_idpar,
					geom,
					area
				ON CONFLICT DO NOTHING;---évite les duplicats en cas d'erreur
				\$\$,
				table_filiation_mY,
				idfil_filiation_mX,
				mX_idpar,
				mY_idpar,
				mY_idprocpte,
				mZ_idpar,
				dep,
				m1
			);
			COMMIT;
			
		END LOOP;

		DROP TABLE anaseq.reste_mX_nodfi CASCADE;

		
		------------------------------------------
		---Suppression des surfaces assimilées à du bruit
		-------------------------------------------
		RAISE NOTICE 'SUPPRESSION DU BRUIT';

		EXECUTE format(
			\$\$
			DELETE FROM anaseq.%I
			WHERE area < 10
			\$\$,
			table_filiation_mY);
		COMMIT;


		---Suppression des tables temporaires
		EXECUTE format(
			\$\$
			DROP TABLE IF EXISTS anaseq.mx_nodfi_uni CASCADE;
			DROP TABLE IF EXISTS anaseq.mx_dfi_retrouve_uni CASCADE;
			DROP TABLE IF EXISTS anaseq.mx_diff CASCADE;
			DROP TABLE IF EXISTS anaseq.mx_nodfi_etape2 CASCADE;
			DROP TABLE IF EXISTS anaseq.par_mZ CASCADE;
			DROP TABLE IF EXISTS anaseq.mx_non_cadastre CASCADE;
			\$\$);
		COMMIT;

		X := X + 1; -- Incrémentation de X
		mX := ((mX::numeric)-1)::text;
 
	END LOOP;

	------------------------------------------
	---Insert dans la table multi-millésime antérieure
	-------------------------------------------
	RAISE NOTICE 'INSERT FINAL DU DEPARTEMENT % DANS LA TABLE MULTI-MILLESIME ANTERIEURE',dep;
	
	EXECUTE format(
		\$\$
		INSERT INTO anaseq.%I
		SELECT com_m1.annee,
			filiation_m6.idpk,
			com_m1.ccodep,
			com_m1.iddep22,
			com_m1.idcom22,
			com_m1.com_niv,
			com_m1.scorpat,
			com_m1.typpat,
			com_m1.m1_idpar,
			filiation_m2.m2_idpar,
			filiation_m3.m3_idpar,
			filiation_m4.m4_idpar,
			filiation_m5.m5_idpar,
			filiation_m6.m6_idpar,
			com_m1.m1_idprocpte,
			filiation_m2.m2_idprocpte,
			filiation_m3.m3_idprocpte,
			filiation_m4.m4_idprocpte,
			filiation_m5.m5_idprocpte,
			filiation_m6.m6_idprocpte,
			filiation_m6.geom,
			filiation_m6.area
		FROM anaseq.com_m1
		JOIN anaseq.filiation_m2 ON com_m1.idpk = filiation_m2.idfil_m1
		JOIN anaseq.filiation_m3 ON filiation_m2.idpk = filiation_m3.idfil_m2
		JOIN anaseq.filiation_m4 ON filiation_m3.idpk = filiation_m4.idfil_m3
		JOIN anaseq.filiation_m5 ON filiation_m4.idpk = filiation_m5.idfil_m4
		JOIN anaseq.filiation_m6 ON filiation_m5.idpk = filiation_m6.idfil_m5
		\$\$,
		table_filiation_anterieure_dep
		);
	COMMIT;

	EXECUTE format(
		\$\$
		DROP TABLE IF EXISTS anaseq.com_m1 CASCADE;
		DROP TABLE IF EXISTS anaseq.filiation_m2 CASCADE;
		DROP TABLE IF EXISTS anaseq.filiation_m3 CASCADE;
		DROP TABLE IF EXISTS anaseq.filiation_m4 CASCADE;
		DROP TABLE IF EXISTS anaseq.filiation_m5 CASCADE;
		DROP TABLE IF EXISTS anaseq.filiation_m6 CASCADE;
		\$\$);
	COMMIT;
	
END LOOP;

EXECUTE format(
	\$\$
	DROP TABLE anaseq.dep;
	\$\$);

END
\$do\$
