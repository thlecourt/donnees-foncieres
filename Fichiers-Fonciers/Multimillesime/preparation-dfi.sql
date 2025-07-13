----------
--- Ce script prépare les Documents de Filiation Informatisés (DFI) pour une jointure avec les Fichiers Fonciers
----------

--- On sélectionne les modifications pertinentes à notre période et terrain d'étude
DROP TABLE IF EXISTS dfi.dfi_ff CASCADE;
CREATE TABLE dfi.dfi_ff AS (
	SELECT * FROM dfi.dfi_source
	WHERE LEFT(date::text,4)::numeric > 2008
		AND LEFT(date::text,4)::numeric < 2021
		AND dep03 NOT LIKE '97%'
);

---On fusionne et nettoie les colonnes id_par en une seule agrégée
ALTER TABLE dfi.dfi_ff ADD COLUMN iddep varchar(2);
ALTER TABLE dfi.dfi_ff ADD COLUMN date2 date;
UPDATE dfi.dfi_ff SET iddep = LEFT(dep03,2);
UPDATE dfi.dfi_ff SET date2 =
	TO_DATE(LEFT(date::text,4)::int||'-'||
		LEFT(RIGHT(date::text,4),2)::int||'-'||
		RIGHT(date::text,2)::int,
		'YYYY-MM-DD');

---On repère le non-cadastré (idpar vide)
UPDATE dfi.dfi_ff SET l_idpar = ARRAY['NC']
WHERE l_idpar[1] IS NULL;


---On crée la table désagrégée (1 ligne = 1 modification d'1 parcelle en 1 parcelle)
DROP TABLE IF EXISTS dfi.unnest;
CREATE TABLE dfi.unnest AS (
	WITH sub AS (
		SELECT t1.iddep,
			t1.codcom,
			t1.id_dfi,
			t1.prefsec,
			t1.nlot,
			t1.date2 date,
			UNNEST(t1.l_idpar) idpar_mere,
			t2.l_idpar l_idpar_fille
		FROM dfi.dfi_ff t1
		JOIN dfi.dfi_ff t2 ON t1.id_dfi = t2.id_dfi
			AND t1.prefsec = t2.prefsec
			AND t1.codcom = t2.codcom
			AND t1.nlot = t2.nlot
			AND t1.iddep = t2.iddep
		WHERE t1.type = 1
			AND t2.type = 2),

		sub2 AS (
		SELECT iddep,
			codcom,
			prefsec,
			id_dfi,
			nlot,
			date,
			regexp_replace(idpar_mere,'\s+', '') idpar_mere,
			UNNEST(l_idpar_fille) AS idpar_fille
		FROM sub
		)

	SELECT CONCAT(date,iddep,codcom,id_dfi,prefsec,nlot) id_filiation,
		iddep,
		codcom,
		prefsec,
		date,
		idpar_mere,
		regexp_replace(idpar_fille,'\s+', '') idpar_fille
	FROM sub2);
	
DROP TABLE dfi.dfi_ff CASCADE;



---On qualifie le type de filiation
ALTER TABLE dfi.unnest DROP COLUMN IF EXISTS idpk CASCADE;
ALTER TABLE dfi.unnest DROP COLUMN IF EXISTS type CASCADE;
ALTER TABLE dfi.unnest ADD COLUMN idpk serial PRIMARY KEY;
ALTER TABLE dfi.unnest ADD COLUMN type text;

UPDATE dfi.unnest dfi
SET type =
	CASE
		WHEN idpar_mere LIKE 'NC' AND idpar_fille NOT LIKE 'NC' THEN 'apparition'
		WHEN idpar_fille LIKE 'NC' AND idpar_mere NOT LIKE 'NC' THEN 'disparition'
		WHEN idpar_mere LIKE 'NC' AND idpar_fille LIKE 'NC' THEN 'non-cadastré à non-cadastré'
	END;


DROP TABLE IF EXISTS dfi.temp_filiation CASCADE;
CREATE TABLE dfi.temp_filiation AS (
WITH sub AS (
	SELECT id_filiation, idpar_mere, COUNT(idpar_fille) nfilles
	FROM dfi.unnest
	GROUP BY id_filiation, idpar_mere),

	sub2 AS (
	SELECT id_filiation, idpar_fille, COUNT(idpar_mere) nmeres
	FROM dfi.unnest
	GROUP BY id_filiation, idpar_fille)
	
	SELECT sub.id_filiation, sub.nfilles, sub2.nmeres
	FROM sub
	JOIN sub2 ON sub.id_filiation = sub2.id_filiation
	GROUP BY sub.id_filiation, nfilles, nmeres);

CREATE INDEX temp_id_filiation ON dfi.temp_filiation(id_filiation);
CREATE INDEX dfi_id_filiation ON dfi.unnest(id_filiation);

UPDATE dfi.unnest dfi
SET type =
	CASE
		WHEN temp.nfilles > 1 AND temp.nmeres = 1 THEN 'division'
		WHEN temp.nfilles = 1 AND temp.nmeres > 1 THEN 'fusion'
		WHEN temp.nfilles = 1 AND temp.nmeres = 1 THEN 'transfert'
	END
FROM dfi.temp_filiation temp
WHERE dfi.type IS NULL
	AND dfi.id_filiation = temp.id_filiation;

DROP TABLE IF EXISTS dfi.temp_filiation CASCADE;


----Création des idpar
ALTER TABLE dfi.unnest DROP COLUMN IF EXISTS idpar_mere_normal CASCADE;
ALTER TABLE dfi.unnest DROP COLUMN IF EXISTS idpar_mere_inverse CASCADE;
ALTER TABLE dfi.unnest DROP COLUMN IF EXISTS idpar_fille_normal CASCADE;
ALTER TABLE dfi.unnest DROP COLUMN IF EXISTS idpar_fille_inverse CASCADE;
ALTER TABLE dfi.unnest ADD COLUMN idpar_mere_normal text;
ALTER TABLE dfi.unnest ADD COLUMN idpar_mere_inverse text;
ALTER TABLE dfi.unnest ADD COLUMN idpar_fille_normal text;
ALTER TABLE dfi.unnest ADD COLUMN idpar_fille_inverse text;

CREATE INDEX idpar_mere_normal_idx ON dfi.unnest(idpar_mere_normal);
CREATE INDEX idpar_mere_inverse_idx ON dfi.unnest(idpar_mere_inverse);
CREATE INDEX idpar_fille_normal_idx ON dfi.unnest(idpar_fille_normal);
CREATE INDEX idpar_fille_inverse_idx ON dfi.unnest(idpar_fille_inverse);

UPDATE dfi.unnest SET
idpar_mere_normal =
	CASE
		WHEN idpar_mere LIKE 'NC' THEN 'NC'
		ELSE CONCAT(iddep,codcom,prefsec,idpar_mere)
	END,
idpar_fille_normal =
	CASE
		WHEN idpar_fille LIKE 'NC' THEN 'NC'
		ELSE CONCAT(iddep,codcom,prefsec,idpar_fille)
	END,
idpar_mere_inverse =
	CASE
		WHEN idpar_mere NOT LIKE 'NC' AND prefsec NOT LIKE '000'
			THEN CONCAT(iddep,prefsec,'000',idpar_mere)
	END,
idpar_fille_inverse =
	CASE
		WHEN idpar_fille NOT LIKE 'NC' AND prefsec NOT LIKE '000'
			THEN CONCAT(iddep,prefsec,'000',idpar_fille)
	END;
