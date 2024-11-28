use cog_idp_syncer::sync_request::Body;
use cog_idp_syncer::sync_response::Body as ResponseBody;

use cog_idp_syncer::cog_idp_to_gandalf_syncer_client::CogIdpToGandalfSyncerClient;
use cog_idp_syncer::{ProjectsRequest, SyncRequest};

pub mod cog_idp_syncer {
    tonic::include_proto!("cog_idp_syncer");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = CogIdpToGandalfSyncerClient::connect("http://[::1]:50051").await?;

    let request = tonic::Request::new(SyncRequest {
        body: Some(Body::Projects(ProjectsRequest { cursor: None })),
    });

    let request = client.sync(request).await?;

    let mut inbound = request.into_inner();

    while let Some(response) = inbound.message().await? {
        println!("RESPONSE={:?}", response);
        match response.body {
            Some(ResponseBody::Project(project_sync_response)) => {
                println!("PROJECT RESPONSE={:?}", project_sync_response);
            }
            Some(ResponseBody::Token(_)) => unreachable!(),
            None => todo!(),
        }
    }

    Ok(())
}
