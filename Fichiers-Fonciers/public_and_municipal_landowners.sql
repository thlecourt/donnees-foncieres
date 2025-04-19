--------------------------------
Ce script identifie les propriétaires publics et communaux à partir des Fichiers Fonciers et de données exogènes, et notamment de la base SIRENE (INSEE)
--------------------------------

---Création des colonnes de qualification
ALTER TABLE ff.proprietaires_multimillesime
ADD COLUMN public boolean;
ALTER TABLE ff.proprietaires_multimillesime
ADD COLUMN cominterco boolean;


---Affectation des cas simples
UPDATE ff.proprietaires_multimillesime
SET public = 'True'
WHERE cod_prop SIMILAR TO 'P%|F2%|F4%|F5b|F7a|F7c|F7g|A1g|A2c|A3%|A4%|R2%|R4%|R5b|E2%|S1c|S1d|S2%|Z4%|L1c|L3a|M2a';

UPDATE ff.proprietaires_multimillesime
SET cominterco = 'True'
WHERE cod_prop SIMILAR TO 'P4%|P5%|P6%|F2%|F4%|F5b|S2b|L1c';



--------------------------------
-----LOGEMENT SOCIAL PUBLIC-----
--------------------------------

UPDATE ff.proprietaires_multimillesime prop
SET public = 'True'
FROM sirene_2021.siren_unitelegale siren
WHERE prop.dsiren = siren.siren AND prop.public IS NOT TRUE
	AND prop.cod_prop LIKE 'F1a'
	AND siren.categoriejuridique LIKE '7371%';

----A partir de <https://www.foph.fr/oph/Les-Offices/>---
UPDATE ff.proprietaires_multimillesime prop
SET public = 'True'
FROM prop_mm.liste_oph o
WHERE prop.cod_prop LIKE 'F1a' AND public IS NOT TRUE
	AND to_tsvector('french', SPLIT_PART(prop.nom_adresse, '|', 1)) @@ plainto_tsquery('french', o.nom)
	AND LEFT(SPLIT_PART(prop.nom_adresse, '|', 5),5) = o.cp;

UPDATE ff.proprietaires_multimillesime
SET public = 'True'
WHERE cod_prop LIKE 'F1a' AND public IS NOT TRUE
	AND SPLIT_PART(nom_adresse, '|', 1) SIMILAR TO 'OPH %|% OPH|% OPH %|OPHLM %|% OPHLM|% OPHLM %|%OP HLM%|%O P HLM%|%OPAC%|% PUBLIC|
		% PUBLIC %|% PUBLIQUE|% PUBLIQUE %|%MUNICIP%|%OFFICE%DEPARTEMENT%|%COMMUNAL%|%COMMUNE%|%COMMUNAUTE%AGGLO%|%COMMUNAUTE%URBAIN%'
	AND SPLIT_PART(nom_adresse, '|', 1) NOT SIMILAR TO 'ESH %|% ESH|% ESH %|%ENTREPRISE%|%COOP%|SCIC %|% SCIC|% SCIC %';

UPDATE ff.proprietaires_multimillesime
SET cominterco = 'True'
WHERE public IS TRUE AND cod_prop LIKE 'F1a'
	AND (SPLIT_PART(nom_adresse, '|', 1) SIMILAR TO '%MUNICIP%|%COMMUNAL%|%COMMUNE%|%AGGLO%|%METROPOL%|%VILLE%|%COMMUNAUTE%URBAIN%'
		OR (SPLIT_PART(nom_adresse, '|', 2|3) SIMILAR TO '%HOTEL%VILLE%|%MAIR%|%HOTEL%AGGLO%|%HOTEL%COMMUNAUTE%|%HOTEL%METROPOL%'
		   AND SPLIT_PART(nom_adresse, '|', 1) NOT SIMILAR TO 'ASS%'));

UPDATE ff.proprietaires_multimillesime
SET cominterco = 'True'
WHERE public IS TRUE AND idu SIMILAR TO '914774|912077|2337255|529984|4320368|564659|966161|1613493|518967|262195|
	2377566|242577|48681|2326689|2380195|588004|6741656|95400|2456888|1325908|3260193|1325558|1197083|
	2212684|1132741|100282|424969|2365535|2874273|622676|5444893|5764033|2738486|1295452|377638|1195868|
	803384|434077|2395450|677690|1276243|426056|1132774|964871|1174520|2334880|1414030|2008028|2676204|
	2355512|3059579|1547066|2398522|2761837|2319568|2292466|2300332|2329839|1215920|2551307|18771|1531407|
	2384817|6217907|1331544|6404513|2911468|6858457|1547831|5026984|3411551|2976728|2665414|2294901|2298685|
	1990622|1009416|1420289|5054373|2335683|6453516|5761770|2982000|2378557|43167|6203066|2644379|4788563|
	2664000|2301534|1592491|1968089|2261766|2313047|1588036|1931682|1826766|1877741|3152442|2221937|2927811|
	1420282|2121813|2093077|1596184|1968058|1249133|1256129|803384|1990622|2355512|2384817|160179|2325811|
	2664000';



--------------------------------
----- REGIES PUBLIQUES (<https://www.services.eaufrance.fr/donnees/telechargement>)
----- versions 2011 et 2021 agrégées
--------------------------------

UPDATE ff.proprietaires_multimillesime prop
SET public = 'True'
FROM prop_mm.eauass_regie_publique_1120 eau
WHERE prop.dsiren = eau.siren;

UPDATE ff.proprietaires_multimillesime prop
SET public = 'True'
WHERE cod_prop SIMILAR TO 'R6%' AND public IS NOT TRUE
AND (SPLIT_PART(prop.nom_adresse, '|', 1) SIMILAR TO '%REGIE%|% PUBLIC|% PUBLIC %|% PUBLIQUE|% PUBLIQUE %|
	%MUNICIP%|%DEPARTEMENT%|%COMMUNAL%|%COMMUNE%|%COMMUNAUTE%AGGLO%|%METROPOL%|%COMMUNAUTE%URBAIN%'
	 OR (SPLIT_PART(prop.nom_adresse, '|', 2|3) SIMILAR TO '%HOTEL%VILLE%|%MAIR%|%HOTEL%AGGLO%|%HOTEL%COMMUNAUTE%|%HOTEL%DEPARTEMENT%|%HOTEL%REGION%|%HOTEL%METROPOL%'
		AND SPLIT_PART(nom_adresse, '|', 1) NOT SIMILAR TO 'ASS%'));
		 
UPDATE ff.proprietaires_multimillesime prop
SET cominterco = 'True'
FROM prop_mm.eauass_regie_publique_1120 eau
WHERE prop.dsiren = eau.siren AND prop.public IS TRUE AND prop.cod_prop LIKE 'R6a'
	AND (eau.type NOT SIMILAR TO 'Département|Inconnu|Syndicat de départements'
		 OR (eau.type SIMILAR TO 'Syndicat Mixte' AND (SPLIT_PART(prop.nom_adresse, '|', 1) SIMILAR TO '%MUNICIP%|%COMMUNAL%|%COMMUNE%|%COMMUNAUTE%AGGLO%|%METROPOL%|%VILLE%|%COMMUNAUTE%URBAIN%'
		OR (SPLIT_PART(prop.nom_adresse, '|', 2|3) SIMILAR TO '%HOTEL%VILLE%|%MAIR%|%HOTEL%AGGLO%|%HOTEL%COMMUNAUTE%|%HOTEL%METROPOL%'
		   AND SPLIT_PART(prop.nom_adresse, '|', 1) NOT SIMILAR TO 'ASS%'))));

UPDATE ff.proprietaires_multimillesime prop
SET cominterco = 'True'
WHERE cod_prop SIMILAR TO 'R6%' AND public IS TRUE AND cominterco IS NOT TRUE
AND (SPLIT_PART(prop.nom_adresse, '|', 1) SIMILAR TO '%MUNICIP%|%COMMUNAL%|%COMMUNE%|%COMMUNAUTE%AGGLO%|%METROPOL%|%VILLE%|%COMMUNAUTE%URBAIN%'
	 OR (SPLIT_PART(prop.nom_adresse, '|', 2|3) SIMILAR TO '%HOTEL%VILLE%|%MAIR%|%HOTEL%AGGLO%|%HOTEL%COMMUNAUTE%|%HOTEL%METROPOL%'
		AND SPLIT_PART(prop.nom_adresse, '|', 1) NOT SIMILAR TO 'ASS%'));



--------------------------------
--- ETABLISSEMENTS PUBLICS DE SANTE (https://www.data.gouv.fr/fr/datasets/finess-extraction-du-fichier-des-etablissements/)
----- versions 2011 et 2021 agrégées
--------------------------------

UPDATE ff.proprietaires_multimillesime prop
SET public = 'True'
FROM prop_mm.etab_sante_1120 sant
WHERE sant.sph = 1 AND prop.dsiren = LEFT(sant.siret,9);

UPDATE ff.proprietaires_multimillesime prop
SET public = 'True'
WHERE prop.cod_prop SIMILAR TO 'S1a|S1b' AND public IS NOT TRUE
	AND (SPLIT_PART(prop.nom_adresse, '|', 1) SIMILAR TO '% PUBLIC|% PUBLIC %|% PUBLIQUE|% PUBLIQUE %|
	%MUNICIP%|%DEPARTEMENT%|%COMMUNAL%|%COMMUNE%|%COMMUNAUTE%AGGLO%|%METROPOL%|%VILLE%|%COMMUNAUTE%URBAIN%'
	 OR (SPLIT_PART(prop.nom_adresse, '|', 2|3) SIMILAR TO '%HOTEL%VILLE%|%MAIR%|%HOTEL%AGGLO%|%HOTEL%COMMUNAUTE%|%HOTEL%DEPARTEMENT%|%HOTEL%REGION%|%HOTEL%METROPOL%'
		AND SPLIT_PART(prop.nom_adresse, '|', 1) NOT SIMILAR TO 'ASS%'));

UPDATE ff.proprietaires_multimillesime prop
SET cominterco = 'True'
FROM prop_mm.etab_sante_1120 sant
WHERE public IS TRUE AND prop.dsiren = LEFT(sant.siret,9)
	AND sant.nom SIMILAR TO '%MUNICIP%|%COMMUNAL%|%COMMUNE%|%COMMUNAUTE%AGGLO%|%METROPOL%|%VILLE%|%COMMUNAUTE%URBAIN%';

UPDATE ff.proprietaires_multimillesime prop
SET cominterco = 'True'
WHERE prop.cod_prop SIMILAR TO 'S1a|S1b' AND public IS TRUE AND cominterco IS NOT TRUE
	AND	(SPLIT_PART(prop.nom_adresse, '|', 1) SIMILAR TO '%MUNICIP%|%COMMUNAL%|%COMMUNE%|%COMMUNAUTE%AGGLO%|%METROPOL%|%VILLE%|%COMMUNAUTE%URBAIN%'
	 OR (SPLIT_PART(prop.nom_adresse, '|', 2|3) SIMILAR TO '%HOTEL%VILLE%|%MAIR%|%HOTEL%AGGLO%|%HOTEL%COMMUNAUTE%|%HOTEL%METROPOL%'
		AND SPLIT_PART(prop.nom_adresse, '|', 1) NOT SIMILAR TO 'ASS%'));


--------------------------------
---- ENSEIGNEMENT SUPERIEUR
--- ((https://data.enseignementsup-recherche.gouv.fr/explore/dataset/fr-esr-principaux-etablissements-enseignement-superieur/information/?disjunctive.type_d_etablissement&disjunctive.typologie_d_universites_et_assimiles)
--------------------------------

UPDATE ff.proprietaires_multimillesime prop
SET public = 'True'
FROM prop_mm.etab_ensup_21 ensup
WHERE ensup.secteur LIKE 'Public'
	AND prop.dsiren = ensup.siren;



--------------------------------
---- ENSEIGNEMENT PRIMAIRE ET SECONDAIRE ET AGRICOLE ---
--------------------------------

UPDATE ff.proprietaires_multimillesime prop
SET public = 'True'
FROM sirene_2021.siren_unitelegale siren
WHERE prop.dsiren = siren.siren AND prop.public IS NOT TRUE
	AND prop.cod_prop LIKE 'E3%'
	AND siren.categoriejuridique SIMILAR TO '41%|71%|72%|73%|74%';

UPDATE ff.proprietaires_multimillesime prop
SET public = 'True'
WHERE prop.cod_prop LIKE 'E3%' AND prop.public IS NOT TRUE
	AND (SPLIT_PART(prop.nom_adresse, '|', 1) SIMILAR TO '% PUBLIC|% PUBLIC %|% PUBLIQUE|% PUBLIQUE %|
	%MUNICIP%|%DEPARTEMENT%|%COMMUNAL%|%COMMUNE%|%COMMUNAUTE%AGGLO%|%METROPOL%|%VILLE%|%COMMUNAUTE%URBAIN%'
	 OR (SPLIT_PART(prop.nom_adresse, '|', 2|3) SIMILAR TO '%HOTEL%VILLE%|%MAIR%|%HOTEL%AGGLO%|%HOTEL%COMMUNAUTE%|%HOTEL%DEPARTEMENT%|%HOTEL%REGION%|%HOTEL%METROPOL%'
		AND SPLIT_PART(prop.nom_adresse, '|', 1) NOT SIMILAR TO 'ASS%'))
	AND SPLIT_PART(prop.nom_adresse, '|', 1) NOT SIMILAR TO '%PRIVE%|%CATHOLIQ%|%ASSOCIATI%|%RELIGI%';

UPDATE ff.proprietaires_multimillesime prop
SET cominterco = 'True'
FROM sirene_2021.siren_unitelegale siren
WHERE prop.dsiren = siren.siren AND prop.cod_prop LIKE 'E3a' AND prop.public IS TRUE
	AND siren.categoriejuridique SIMILAR TO '7210%|731%|732%|734%|7353%|7354%|7355%|7356%|7357%|
	7361%|7362%|7363%|7366%|7367%|7371%|7378%|7379%';

UPDATE ff.proprietaires_multimillesime prop
SET cominterco = 'True'
WHERE prop.cod_prop LIKE 'E3a' AND prop.public IS TRUE AND cominterco IS NOT TRUE
	AND (SPLIT_PART(prop.nom_adresse, '|', 1) SIMILAR TO '%PRIMAIRE%|%MATERNEL%|%MUNICIP%|%COMMUNE%|%COMMUNAL%|%AGGLO%|%SYNDIC%|%INTERCO%|%METROPOL%|%VILLE%|%COMMUNAUTE%URBAIN%'
		 OR (SPLIT_PART(prop.nom_adresse, '|', 2|3) SIMILAR TO '%MAIR%|%HOTEL%VILLE|%HOTEL%AGGLO%|%HOTEL%COMMUNAUTE%|%HOTEL%METROPOL'
			AND SPLIT_PART(prop.nom_adresse, '|', 1) NOT SIMILAR TO 'ASS%'))
	AND SPLIT_PART(prop.nom_adresse, '|', 1) NOT SIMILAR TO '%LYCEE%|%COLLEG%|%UNIVERSIT%';

--- Autres personnes morales
UPDATE ff.proprietaires_multimillesime prop
SET public = 'True', cominterco = 'True'
WHERE cod_prop LIKE 'M2a' AND SPLIT_PART(prop.nom_adresse, '|', 1)
	   SIMILAR TO '%MUNICIP%|%COMMUNAL%|%COMMUNE%|%COMMUNAUTE%AGGLO%|%METROPOL%|%VILLE%|%COMMUNAUTE%URBAIN%';



--------------------------------
----- REDRESSEMENTS ET CORRECTIONS MANUELLES
--------------------------------

UPDATE ff.proprietaires_multimillesime
SET cod_prop = 'M0a', public = 'False', cominterco = 'False'
WHERE idu SIMILAR TO '393145';

UPDATE ff.proprietaires_multimillesime
SET cod_prop = 'P3a', public = 'True', cominterco = 'False'
WHERE cod_prop LIKE 'P5a' AND SPLIT_PART(nom_adresse, '|', 1) SIMILAR TO '%DEPARTEMENTAL%';

UPDATE ff.proprietaires_multimillesime
SET cod_prop = 'A3g', public = 'False', cominterco = 'False'
WHERE idu SIMILAR TO '3406396|5497220';

UPDATE ff.proprietaires_multimillesime
SET cod_prop = 'S2b', public = 'True', cominterco = 'True'
WHERE cod_prop SIMILAR TO 'P5a|P6a' AND SPLIT_PART(nom_adresse, '|', 1) SIMILAR TO '%SOCIAL%|CCAS %|% CCAS|% CCAS %|%ACTION SOC%';

UPDATE ff.proprietaires_multimillesime
SET cod_prop = 'A5a', public = 'False', cominterco = 'False'
WHERE cod_prop SIMILAR TO 'P5a|M1a' AND SPLIT_PART(nom_adresse, '|', 1) SIMILAR TO '% AFR|AFR %|% AFR %|%REMEMBREMEMT%|%FONCIERE%'
	AND idu NOT SIMILAR TO '4841359|2193333|1904393';

UPDATE ff.proprietaires_multimillesime
SET cod_prop = 'R6a', public = 'True', cominterco = 'True'
WHERE idu SIMILAR TO '461343';

UPDATE ff.proprietaires_multimillesime
SET cod_prop = 'F4b', public = 'True', cominterco = 'True'
WHERE idu SIMILAR TO '2401781|2409386|1564550|1507849|2267132|805676|2328203|2767663|1575502|2609853|18165|2411627|2279950'
 OR (cod_prop SIMILAR TO 'F4c|F6a|M1a|R6a' AND SPLIT_PART(nom_adresse, '|', 1)
		SIMILAR TO 'SPL %|% SPL|% SPL %|SPLA %|% SPLA|% SPLA %|%SOCIETE PUBLIQUE LOCALE%'
	 AND idu NOT SIMILAR TO '935895|6684072');

UPDATE ff.proprietaires_multimillesime
SET cod_prop = 'P4d', public = 'True', cominterco = 'True'
WHERE idu SIMILAR TO '4688303|1786340|5422610|960451';

UPDATE ff.proprietaires_multimillesime
SET cod_prop = 'M1a', public = 'False', cominterco = 'False'
WHERE idu = '238840';

UPDATE ff.proprietaires_multimillesime
SET cod_prop = 'P4b', public = 'True', cominterco = 'True'
WHERE idu SIMILAR TO '2173228|508198|1528330';

UPDATE ff.proprietaires_multimillesime
SET cod_prop = 'G2a', public = 'False', cominterco = 'False'
WHERE idu = '148325';

UPDATE ff.proprietaires_multimillesime
SET cod_prop = 'P4c', public = 'True', cominterco = 'True'
WHERE idu = '5053499';

UPDATE ff.proprietaires_multimillesime
SET cod_prop = 'F2a'
WHERE idu SIMILAR TO '3528773|3539131|3594981|2365181|60320|1827614|2323667|518625|2456216|1015178|6233897|6581013|
	2593406|167011|2439468|1289689|3346530|3158743|1214544|2536809|2719472|363687|3378738|3463791|2091605|154650|3244976|60320|150412
	2173743|201218|2454567|1214544';

UPDATE ff.proprietaires_multimillesime
SET cod_prop = 'F2b'
WHERE cod_prop = 'F2a' AND idu NOT SIMILAR TO '2365181|60320|1827614|2323667|518625|2456216|1015178|6233897|6581013|
	2593406|167011|2439468|1289689|3346530|3158743|1214544|2536809|2719472|363687|3378738|3463791|2091605|154650|3244976|60320|150412
	2173743|201218|2454567|1214544';

UPDATE ff.proprietaires_multimillesime
SET cod_prop = 'F5b'
WHERE cominterco IS TRUE AND cod_prop LIKE 'F2%'
	AND SPLIT_PART(nom_adresse, '|', 1) SIMILAR TO '%REGION%PARISIENNE|%AFTRP%';


---Propagation des idu identifiés aux id correspondants
UPDATE ff.proprietaires_multimillesime
SET public = 'True'
WHERE public IS NOT TRUE AND idu IN
	(SELECT DISTINCT idu
	FROM ff.proprietaires_multimillesime
	WHERE public IS TRUE);
UPDATE ff.proprietaires_multimillesime
SET cominterco = 'True'
WHERE cominterco IS NOT TRUE AND idu IN
	(SELECT DISTINCT idu
	FROM ff.proprietaires_multimillesime
	WHERE cominterco IS TRUE);
	
	
---Corrections de catégories divergentes pour un même idu

UPDATE ff.proprietaires_multimillesime
SET cod_prop = 'F4b'
WHERE cod_prop SIMILAR TO 'F6a|M1a' AND cominterco IS TRUE;

UPDATE ff.proprietaires_multimillesime
SET public = 'False', cominterco = 'False'
WHERE cod_prop LIKE 'A5a' AND cominterco IS TRUE;

UPDATE ff.proprietaires_multimillesime
SET cod_prop = 'P4d'
WHERE cod_prop SIMILAR TO 'M2a' AND cominterco IS TRUE
	AND (SPLIT_PART(nom_adresse, '|', 1) SIMILAR TO '%SYND%') OR idu LIKE '3683436';

UPDATE ff.proprietaires_multimillesime
SET cod_prop = 'R5b'
WHERE cod_prop SIMILAR TO 'M2a' AND cominterco IS TRUE
	AND (SPLIT_PART(nom_adresse, '|', 1) SIMILAR TO '%REGIE%' OR idu LIKE '1565851');

UPDATE ff.proprietaires_multimillesime
SET cominterco = 'False'
WHERE cod_prop SIMILAR TO 'M2a|P3a|S1d|S2e' AND cominterco IS TRUE
	AND SPLIT_PART(nom_adresse, '|', 1) NOT SIMILAR TO '%MUNICIP%|%COMMUNAL%|%COMMUNE%|%COMMUNAUTE%AGGLO%|%METROPOL%|%VILLE%|%COMMUNAUTE%URBAIN%'
	AND idu NOT SIMILAR TO '5761463|';

UPDATE ff.proprietaires_multimillesime
SET cod_prop = 'P5a'
WHERE cod_prop SIMILAR TO 'M2a' AND cominterco IS TRUE
	AND idu LIKE '1546987';

UPDATE ff.proprietaires_multimillesime
SET cod_prop = 'L1c'
WHERE cod_prop SIMILAR TO 'M2a' AND cominterco IS TRUE
	AND idu SIMILAR TO '742314|4949092';

UPDATE ff.proprietaires_multimillesime
SET public = 'False', cominterco = 'False'
WHERE idu SIMILAR TO '2675564|6008287|4127826|5697924|5085859|5187455|4830992|1428782|1638376|5834385'
	OR (cominterco IS TRUE AND cod_prop LIKE 'S2e');

UPDATE ff.proprietaires_multimillesime
SET cod_prop = 'G2a', public = 'False', cominterco = 'False'
WHERE cominterco IS TRUE AND cod_prop SIMILAR TO 'M2a|P4d' AND SPLIT_PART(nom_adresse, '|', 1) SIMILAR TO 'ASL%|% ASL %|% ASL|A S L%|% A S L%|% A S L|%ASS% SYND%';

UPDATE ff.proprietaires_multimillesime
SET cod_prop = 'Z4a', public = 'True', cominterco = 'False'
WHERE cominterco IS TRUE AND cod_prop LIKE 'M2a' AND idu SIMILAR TO '4106965';

UPDATE ff.proprietaires_multimillesime
SET cod_prop = 'F7b', public = 'False', cominterco = 'False'
WHERE cominterco IS TRUE AND cod_prop LIKE 'M2a' AND idu SIMILAR TO '3598491';

UPDATE ff.proprietaires_multimillesime
SET cod_prop = 'P4d'
WHERE cominterco IS TRUE AND cod_prop LIKE 'M2a' AND idu SIMILAR TO '3405037';

UPDATE ff.proprietaires_multimillesime
SET cod_prop = 'P5a'
WHERE cod_prop LIKE 'G2d' AND idu LIKE '358638'

UPDATE ff.proprietaires_multimillesime
SET public = 'False', cominterco = 'False'
WHERE public IS TRUE AND cod_prop SIMILAR TO 'A3g|G1a|G1d|G2d|R7a|Z1a';

UPDATE ff.proprietaires_multimillesime
SET cominterco = 'True'
WHERE id = 3683436



----------------
---Catégorisation des types de propriétaires communaux
----------------
ALTER TABLE ff.proprietaires_multimillesime DROP COLUMN IF EXISTS com_niv;
ALTER TABLE ff.proprietaires_multimillesime DROP COLUMN IF EXISTS com_niv_libelle;
ALTER TABLE ff.proprietaires_multimillesime
ADD COLUMN com_niv int;
ALTER TABLE ff.proprietaires_multimillesime
ADD COLUMN com_niv_libelle text;

UPDATE ff.proprietaires_multimillesime
SET com_niv = 1, com_niv_libelle = 'Propriétaire générique'
WHERE cominterco IS TRUE AND cod_prop SIMILAR TO 'P4a|P5%|P6%';

UPDATE ff.proprietaires_multimillesime
SET com_niv = 2, com_niv_libelle = 'Propriétaire dédié à la maîtrise foncière'
WHERE cominterco IS TRUE AND cod_prop SIMILAR TO 'F2%|F4%';

UPDATE ff.proprietaires_multimillesime
SET com_niv = 3, com_niv_libelle = 'Propriétaire fléché vers un service'
WHERE cominterco IS TRUE AND cod_prop SIMILAR TO 'F1a|P4b|P4c|P4d|R5b|R6a|E3a|L1c|M2a|S1a|S1b|S2b';
