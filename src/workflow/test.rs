use futures::future::Either;
use reqwest::Url;

use super::{WorkflowContext, WorkflowError, WorkflowResult};

pub struct Time;

impl Time {
    pub async fn sleep(&self, duration: time::Duration) -> () {
        tokio::time::sleep(duration.try_into().unwrap()).await
    }
}

#[derive(Clone)]
pub struct HttpService(Url);

impl HttpService {
    pub async fn put(&self, (id, state): (u32, String)) -> String {
        let url = self
            .0
            .join(&id.to_string())
            .map_err(|e| e.to_string())
            .unwrap();
        match reqwest::Client::new()
            .put(url)
            .body(state.clone())
            .send()
            .await
            .map_err(|e| e.to_string())
        {
            Ok(_) => state,
            Err(e) => e,
        }
    }
}

pub async fn test_workflow(context: &mut WorkflowContext, request: usize) -> WorkflowResult<usize> {
    let days = time::Duration::days;
    let sleep = context.import(Time::sleep);
    let http_put = context.import(HttpService::put);
    let eval1 = context.eval(|| 666)?;
    let wait1 = sleep(days(1));
    let http_response = http_put((42, format!("{request} {eval1}")));
    wait1.await?;
    let wait2 = sleep(days(2));
    wait2.await?;
    let eval2 = context.eval(|| 100usize)?;
    std::mem::drop(sleep(days(3)));
    let drop_sleep = sleep(days(3));
    match futures::future::select(drop_sleep, http_response).await {
        Either::Left(_) => Err(WorkflowError::Panic(None)),
        Either::Right((result, _)) => result.map(|r| r.len() + eval2),
    }
}
