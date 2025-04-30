-----------
---Ce script récupère la dernière géométrie parcellaire valide et en simplifie la géométrie
-----------

DO
$do$
DECLARE
        millesime_a_corriger text;
        millesime_short text;
        millesime_de_reference text;
        schema_a_corriger text;
        table_a_corriger text;
        schema_de_reference text;
        table_de_reference text;
        table_de_reference2 text;
        dep varchar(2);
        dep_table record;

BEGIN

        FOREACH millesime_a_corriger IN ARRAY ARRAY['2021','2009','2011','2012','2013','2014','2015','2016','2017','2018','2019','2020']
        LOOP

                RAISE NOTICE 'Correction du millésime %',millesime_a_corriger;

                millesime_short := RIGHT(millesime_a_corriger,2);
                schema_a_corriger := 'nat';
                table_a_corriger := 'public'||millesime_short;

                FOR dep IN EXECUTE 'SELECT DISTINCT LOWER(ccodep) FROM ' || schema_a_corriger || '.' || table_a_corriger ---le LOWER() gère le problème de 2a/2A
                LOOP

                RAISE NOTICE 'Année %, département %',millesime_a_corriger,dep;


                        FOREACH millesime_de_reference IN ARRAY ARRAY['2021','2020','2019','2018','2017','2016','2015','2014','2013','2012','2011','2009']
                        LOOP

                                schema_de_reference := 'ff_'||millesime_de_reference||'_dep';

                                IF millesime_de_reference::numeric < 2018 THEN
                                        table_de_reference2 := 'd'||dep||'_'||millesime_de_reference||'_pnb10_parcelle';
                                ELSE
                                        table_de_reference2 := 'd'||dep||'_fftp_'||millesime_de_reference||'_pnb10_parcelle';
                                END IF;
                                table_de_reference := table_de_reference2;

                                IF millesime_a_corriger::numeric <= millesime_de_reference::numeric THEN

		                        RAISE NOTICE 'Comparaison de % à %',millesime_a_corriger,millesime_de_reference;
				
				                EXECUTE format(
				                                $$
				                                UPDATE %I.%I a
				                                SET lastgeompar = CASE
				                                		WHEN b.vecteur LIKE 'V' THEN ST_MakeValid(ST_SnapToGrid(ST_Simplify(ST_Buffer(ST_CollectionExtract(b.geompar,3),0),0.01),0.01))
				                                		ELSE ST_MakeValid(b.geompar)
				                                		END,
				                                	geomok = CASE
				                                		WHEN b.vecteur LIKE 'V' THEN TRUE
				                                		ELSE FALSE
				                                		END
				                                FROM %I.%I b
				                                WHERE LOWER(a.ccodep) = %L ---le LOWER() gère le problème de 2a/2A
				                                	AND a.cominterco IS TRUE
				                                        AND a.lastgeompar IS NULL
				                                        AND a.idpar = b.idpar
				                                $$,
				                                schema_a_corriger,table_a_corriger,
				                                schema_de_reference, table_de_reference,
				                                dep,
				                                millesime_de_reference, millesime_a_corriger);
                                END IF; 
                                COMMIT; 
                        END LOOP;
                END LOOP;
        END LOOP;

END
$do$
