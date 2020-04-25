use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};

use super::{Error, Config};

static CONSUMER_SUFFIX: &str = "_consumer";

pub struct Messaging {
    connection: lapin::CloseOnDrop<lapin::Connection>,
}

impl Messaging {
    pub async fn connect(config: Config) -> Result<Messaging, Error> {
        let uri = format!(
            "amqp://{user}:{password}@{host}:{port}/{vhost}",
            user = utf8_percent_encode(&config.user, NON_ALPHANUMERIC),
            password = utf8_percent_encode(&config.password, NON_ALPHANUMERIC),
            vhost = utf8_percent_encode(&config.vhost, NON_ALPHANUMERIC),
            host = &config.host,
            port = &config.port,
        );

        let connection = lapin::Connection::connect(
            &uri,
            lapin::ConnectionProperties::default()
        )
            .await
            .map_err(|err| Error::UnableToConnect(err))?;

        let admin_channel = connection.create_channel().await
            .map_err(|err| Error::UnableToConnect(err))?;

        Ok(Messaging { connection })
    }

    // create channel & declare queue, return receive stream
    pub async fn create_queue(&mut self, bind_key: &str)
        -> Result<(), Error>
    {
        let channel = self.connection.create_channel().await
            .map_err(|err| Error::ChannelCreationFailed(err))?;

        let mut consumer = String::with_capacity(bind_key.len() + CONSUMER_SUFFIX.len());
        consumer.insert_str(0, bind_key);
        consumer.insert_str(bind_key.len(), CONSUMER_SUFFIX);

        let queue = channel.basic_consume(
            bind_key,
            &consumer,
            lapin::options::BasicConsumeOptions::default(),
            lapin::types::FieldTable::default(),
        )
            .await
            .map_err(|err| Error::QueueDeclareFailed(err))?;

        Ok(())
    }

    // create channel & return send sink
    pub fn sink(&mut self) {}

    // one shot send, use common channel
    pub async fn send(&self) {}

    // handle anonymous result "topic"
    pub async fn send_and_wait_for_response(&self) {}
}
