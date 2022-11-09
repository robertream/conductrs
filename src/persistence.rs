#[cfg(test)]
mod tests {
    // #[tokio::test]
    // async fn pg_sqlx() {
    //     let pg_options = sqlx::postgres::PgConnectOptions::new()
    //         .host("localhost")
    //         .password("postgres")
    //         .database("postgres")
    //         .username("postgres");
    //
    //     let sql_pool = sqlx::postgres::PgPoolOptions::new()
    //         .max_connections(5)
    //         .connect_with(pg_options)
    //         .await.unwrap();
    //
    //     let pg_schema = "local";
    //     let sql = format!(
    //         "select user_guid from {}.user_prime_trust where prime_trust_account_id=$1",
    //         pg_schema
    //     );
    //
    //     use sqlx::types::Uuid;
    //     let prime_trust_id = Uuid::new_v4();
    //     let id: Uuid = sqlx::query_scalar(sql.as_str())
    //         .bind(prime_trust_id)
    //         .fetch_one(&sql_pool)
    //         .await.unwrap();
    //
    //     assert_eq!(id, prime_trust_id);
    // }
}
