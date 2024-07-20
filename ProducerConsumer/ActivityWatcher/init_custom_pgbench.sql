DO $$ 
DECLARE
    u_id INT;
    user_id INT;
    gender CHAR(1);
    age INT;
    address TEXT;
BEGIN
    FOR p IN 1..10 LOOP  -- Change 10 to the number of users/posts you want to generate
        u_id := (random() * 100000 + 1)::INT;
        gender := CASE WHEN random() > 0.5 THEN 'M' ELSE 'F' END;
        age := (random() * 50 + 18)::INT;  -- Age range from 18 to 68
        address := '123 Main St, Apt ' || (random() * 100 + 1)::INT || ', City, Country';

        INSERT INTO users (username, email, firstname, age, gender, address)
        VALUES ('user_' || u_id, 'user_' || u_id || '@example.com', 'Firstname_' || u_id, age, gender, address)
        RETURNING id INTO user_id;
        
        INSERT INTO posts (user_id, title, content, created_at)
        VALUES (user_id, 'Post ' || p, 'Content of post ' || p, now() - interval '1 day' * p);
    END LOOP;
END $$;

