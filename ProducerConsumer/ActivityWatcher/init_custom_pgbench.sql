
DO $$
BEGIN
    -- Create users table if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = 'users'
    ) THEN
        EXECUTE '
            CREATE TABLE users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(255) UNIQUE NOT NULL,
                email VARCHAR(255) UNIQUE NOT NULL,
                firstname VARCHAR(255) NOT NULL,
                age INT NOT NULL CHECK (age BETWEEN 18 AND 68),
                gender CHAR(1) CHECK (gender IN (''M'', ''F'')),
                address TEXT
            )
        ';
    END IF;

    -- Create posts table if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = 'posts'
    ) THEN
        EXECUTE '
            CREATE TABLE posts (
                id SERIAL PRIMARY KEY,
                user_id INT REFERENCES users(id),
                title VARCHAR(255) NOT NULL,
                content TEXT NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT now()
            )
        ';
    END IF;

    -- Insert a single user with random data
    PERFORM (
        INSERT INTO users (username, email, firstname, age, gender, address)
        VALUES (
            'user_' || (random() * 100000 + 1)::INT,
            'user_' || (random() * 100000 + 1)::INT || '@example.com',
            'Firstname_' || (random() * 100000 + 1)::INT,
            (random() * 50 + 18)::INT,  -- Age range from 18 to 68
            CASE WHEN random() > 0.5 THEN 'M' ELSE 'F' END,
            '123 Main St, Apt ' || ((random() * 100) + 1)::INT || ', City, Country'
        )
        RETURNING id
    ) INTO user_id;

    -- Insert a single post for the newly created user
    INSERT INTO posts (user_id, title, content)
    VALUES (
        user_id,
        'Sample Post Title',
        'This is a sample post content.'
    );

    -- Commit the transaction
    COMMIT;
END $$;
