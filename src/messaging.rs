use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};

use super::{Error, Config};

static CONSUMER_SUFFIX: &str = "_consumer";

pub struct Messaging {
    connection: lapin::CloseOnDrop<lapin::Connection>,
    admin_channel: lapin::CloseOnDrop<lapin::Channel>,
    publish_channel: lapin::CloseOnDrop<lapin::Channel>,
    other_channels: Vec<lapin::CloseOnDrop<lapin::Channel>>,
}

impl Messaging {
    pub async fn connect(config: Config) -> Result<Messaging, Error> {
        log::debug!("Connecting to broker at '{}:{}'", &config.host, &config.port);
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
            .map_err(|err| Error::ChannelCreationFailed(err))?;

        let publish_channel = connection.create_channel().await
            .map_err(|err| Error::ChannelCreationFailed(err))?;

        Ok(Messaging {
            connection,
            admin_channel,
            publish_channel,
            other_channels: Vec::new(),
        })
    }

    // create channel & declare queue, return receive stream
    pub async fn create_queue(&mut self, bind_key: &str)
        -> Result<lapin::Consumer, Error>
    {
        let channel = self.connection.create_channel().await
            .map_err(|err| Error::ChannelCreationFailed(err))?;

        let mut consumer = String::with_capacity(bind_key.len() + CONSUMER_SUFFIX.len());
        consumer.insert_str(0, bind_key);
        consumer.insert_str(bind_key.len(), CONSUMER_SUFFIX);

        log::debug!("Declaring queue '{}'", bind_key);
        channel.queue_declare(
            bind_key,
            lapin::options::QueueDeclareOptions::default(),
            lapin::types::FieldTable::default(),
        )
            .await
            .map_err(|err| Error::QueueDeclareFailed(err))?;

        let stream = channel.basic_consume(
            bind_key,
            &consumer,
            lapin::options::BasicConsumeOptions::default(),
            lapin::types::FieldTable::default(),
        )
            .await
            .map_err(|err| Error::QueueDeclareFailed(err))?;

        self.other_channels.push(channel);

        Ok(stream)
    }

    // create channel & return send sink
    pub async fn sink(&mut self) -> Result<(), Error> {
        Ok(())
    }

    // one shot send, use common channel
    pub async fn send(&self, bind_key: &str, payload: Vec<u8>) -> Result<(), Error> {
        self.publish_channel.basic_publish(
            "",
            &bind_key,
            lapin::options::BasicPublishOptions::default(),
            payload,
            lapin::BasicProperties::default(),
        )
            .await.map_err(|err| Error::QueueDeclareFailed(err))?
            .await.map_err(|err| Error::AckNotReceived(err))?;

        Ok(())
    }

    // handle anonymous result "topic"
    pub async fn send_and_wait_for_response(&self) {}
}
