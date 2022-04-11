use std::time::Duration;

use tokio;

use avantis_utils::redis;
#[tokio::test]
async fn testx() {
    redis::initialize(
        "redis://localhost:6379",
        2,
    )
    .await
    .unwrap();

    let key = "TEST1234";

    redis::del(key).await.unwrap();

    redis::get_or_set_in_background_with_expire(
        &key,
        || async { Ok("HELO".as_bytes().to_vec()) },
        5,
    )
    .await
    .unwrap();
    let result = redis::get::<Vec<u8>>(&key).await.unwrap().unwrap();
    assert_eq!(&result[8..], "HELO".as_bytes());

    tokio::time::sleep(Duration::from_secs(6)).await;

    let result = redis::get_or_set_in_background_with_expire(
        &key,
        || async { Ok("HELO2".as_bytes().to_vec()) },
        5,
    )
    .await
    .unwrap();

    assert_eq!(&result, "HELO".as_bytes());


    tokio::time::sleep(Duration::from_secs(2)).await;
    let result = redis::get::<Vec<u8>>(&key).await.unwrap().unwrap();
    assert_eq!(&result[8..], "HELO2".as_bytes());
}
