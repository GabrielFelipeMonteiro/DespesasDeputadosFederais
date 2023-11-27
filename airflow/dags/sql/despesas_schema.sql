-- remove duplicates from despesas
DELETE FROM despesas
WHERE "id_despesa" NOT IN (
    SELECT MIN("id_despesa")
    FROM despesas
    GROUP BY "urlDocumento"
);