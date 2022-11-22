use std::future::Future;
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::FutureExt;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::workflow::action::{ActionId, ActionType};

pub trait ActionFn<'a, Ctx, In, Out>: Send + Sync {
    type Output: 'a + Future<Output = Out> + Send + Sync;
    fn call(&self, context: &'a Ctx, input: In) -> Self::Output;
}

impl<'a, Ctx, In, Out, Fut, Func> ActionFn<'a, Ctx, In, Out> for Func
where
    Ctx: 'a,
    Fut: 'a + Send + Sync + Future<Output = Out>,
    Func: Send + Sync + Fn(&'a Ctx, In) -> Fut,
{
    type Output = Fut;

    fn call(&self, context: &'a Ctx, input: In) -> Fut {
        self(context, input)
    }
}

pub type ActionError = String;
pub type ActionResult = Result<String, ActionError>;
pub type BoxActionFn<'a> = Box<dyn Fn(String) -> BoxFuture<'a, ActionResult> + 'a>;

pub struct ActionRegistry<'a, Ctx: Send + Sync> {
    context: Arc<Ctx>,
    actions: std::collections::HashMap<&'static str, BoxActionFn<'a>>,
}

impl<'a, Ctx> ActionRegistry<'a, Ctx>
where
    Ctx: Send + Sync,
{
    pub fn new(context: Ctx) -> Self {
        Self {
            context: Arc::new(context),
            actions: Default::default(),
        }
    }

    pub fn action<In, Out, Func>(mut self, func: Func) -> Self
    where
        Ctx: 'a + Send + Sync,
        In: 'a + Send + DeserializeOwned,
        Out: 'a + Serialize,
        Func: 'a + for<'b> ActionFn<'b, Ctx, In, Out>,
    {
        let type_name = std::any::type_name::<Func>();
        let func = Arc::new(func);
        let context = self.context.clone();
        let action_fn = Box::new(move |input: String| {
            let context = context.clone();
            let func = func.clone();
            async move {
                let input = serde_json::from_str::<In>(&input)
                    .map_err(|e| format!("{type_name}({input}): {e:}"))?;
                let output = func.call(&context, input).await;
                Ok(serde_json::to_string(&output).unwrap())
            }
            .boxed()
        });
        self.actions.insert(type_name, action_fn);
        self
    }

    pub async fn execute(&self, action: (ActionType, ActionId, String)) -> ActionResult {
        let (type_name, _action_id, request) = action;
        let action_fn = self
            .actions
            .get(type_name.as_ref())
            .ok_or_else(|| format!("{type_name}: No action function was registered"))?;
        action_fn(request).await
    }
}

#[inline]
pub(crate) fn action_type_name<Ctx, In, Out, Func>(_: Func) -> ActionType
where
    Func: for<'a> ActionFn<'a, Ctx, In, Out>,
{
    std::any::type_name::<Func>().into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions_sorted::assert_eq;

    async fn test_add(initial: &i32, add: i32) -> String {
        let result = initial + add;
        std::future::ready(format!("{initial} + {add} = {result}")).await
    }

    #[tokio::test]
    #[rustfmt::skip]
    async fn execute_actions() {
        let actions = ActionRegistry::new(42).action(test_add);
        let output = actions.execute(("async_workflow::actions::tests::test_add".into(), 0, "24".to_owned())).await;
        assert_eq!(output, Ok("\"42 + 24 = 66\"".to_owned()));
    }
}
