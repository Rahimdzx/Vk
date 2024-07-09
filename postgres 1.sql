-- Создаем таблицу documents
CREATE TABLE documents (
    url TEXT PRIMARY KEY,       -- Уникальный URL документа
    pub_date BIGINT,            -- Время заявляемой публикации документа
    fetch_time BIGINT,          -- Время получения данного обновления документа
    text TEXT,                  -- Текст документа
    first_fetch_time BIGINT     -- Время первого получения документа
);

-- Создаем функцию upsert_document для обновления или вставки документа
CREATE OR REPLACE FUNCTION upsert_document(
    p_url TEXT,
    p_pub_date BIGINT,
    p_fetch_time BIGINT,
    p_text TEXT
) RETURNS VOID AS $$
DECLARE
    existing_doc RECORD;
BEGIN
    -- Проверяем, существует ли документ с заданным URL
    SELECT * INTO existing_doc FROM documents WHERE url = p_url;

    IF existing_doc IS NOT NULL THEN
        -- Обновляем существующий документ
        UPDATE documents
        SET 
            pub_date = LEAST(existing_doc.pub_date, p_pub_date),
            fetch_time = GREATEST(existing_doc.fetch_time, p_fetch_time),
            text = CASE WHEN p_fetch_time > existing_doc.fetch_time THEN p_text ELSE existing_doc.text END,
            first_fetch_time = LEAST(existing_doc.first_fetch_time, p_fetch_time)
        WHERE url = p_url;
    ELSE
        -- Вставляем новый документ
        INSERT INTO documents (url, pub_date, fetch_time, text, first_fetch_time)
        VALUES (p_url, p_pub_date, p_fetch_time, p_text, p_fetch_time);
    END IF;
END;
$$ LANGUAGE plpgsql;
