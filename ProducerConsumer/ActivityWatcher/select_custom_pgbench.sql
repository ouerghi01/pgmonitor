SELECT 
    u.id AS user_id,
    u.username,
    u.email,
    u.firstname,
    u.age,
    u.gender,
    u.address,
    p.id AS post_id,
    p.title,
    p.content,
    p.created_at
FROM 
    users u
JOIN 
    posts p ON u.id = p.user_id
ORDER BY 
    u.id, p.created_at;
