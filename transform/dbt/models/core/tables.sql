WITH league_pts AS (
SELECT
    div,
    "Team 1" AS H_Team,
    "Team 2" AS A_Team,
    cast(substr(FT, 0, 2) as int) AS HG,
    cast(substr(FT, 3, 3) as int) AS AG,
    CASE
        WHEN cast(substr(FT, 0, 2) as int) >
    cast(substr(FT, 3, 3) as int) THEN 3
        WHEN  cast(substr(FT, 0, 2) as int) =
    cast(substr(FT, 3, 3) as int) THEN 1
        ELSE 0
    END AS H_pts,
    CASE
        WHEN cast(substr(FT, 0, 2) as int) <
    cast(substr(FT, 3, 3) as int) THEN 3
        WHEN  cast(substr(FT, 0, 2) as int) =
    cast(substr(FT, 3, 3) as int) THEN 1
        ELSE 0
    END AS A_pts,

FROM {{ ref('fbdat_2324')}}
WHERE div != 'uefa.cl'
),

H_rank AS (
SELECT
    div,
    H_Team AS Team,
    count(1) AS P,
    sum(H_pts) AS Points,
    count_if(H_pts = 3) AS W,
    count_if(H_pts = 1) AS D,
    count_if(H_pts = 0) AS L,
    sum(HG) AS F,
    sum(AG) AS A,
    sum(HG) - sum(AG) AS GD,
FROM league_pts
GROUP BY div, H_Team
),

A_rank AS (
SELECT
    div,
    A_Team AS Team,
    count(1) AS P,
    sum(A_pts) AS Points,
    count_if(A_pts = 3) AS W,
    count_if(A_pts = 1) AS D,
    count_if(A_pts = 0) AS L,
    sum(HG) AS A,
    sum(AG) AS F,
    sum(AG) - sum(HG) AS GD,
FROM league_pts
GROUP BY div, A_Team
)

SELECT
    H.div,
    H.Team,
    row_number() OVER (partition by H.div ORDER BY H.Points + A.Points DESC) AS Position,
    H.P + A.P AS P,
    H.Points + A.Points AS TotPts,
    H.W + A.W AS W,
    H.D + A.D AS D,
    H.L + A.L AS L,
    H.GD + A.GD AS GD,
    H.P AS HP,
    H.Points AS HPts,
    H.W AS HW,
    H.D AS HD,
    H.L AS HL,
    H.GD AS HGD,
    A.P AS AP,
    A.Points AS APts,
    A.W AS AW,
    A.D AS AD,
    A.L AS AL,
    A.GD AS AGD,

FROM H_rank H
INNER JOIN A_rank A
ON H.Team=A.Team

ORDER BY H.div, TotPts DESC