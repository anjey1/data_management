import * as amqp from 'amqplib';
import { Channel, Connection, Message } from 'amqplib';
import { Collection, MongoClient } from 'mongodb';

interface Street {
	streetId: number;
	name: string;
}

interface ParsedMessage {
	streets: Street[];
	city: string;
}

export class ConsumerService {
	private connection: Connection | null = null;
	private channel: Channel | null = null;
	private readonly exchangeName: string;
	private readonly queueName: string;

	private mongoClient: MongoClient;
	private readonly mongoUrl: string = 'mongodb://mongo:27017';
	private readonly dbName: string = 'citiesDB';


	constructor(exchangeName: string, queueName: string) {
		this.exchangeName = exchangeName;
		this.queueName = queueName;
		this.mongoClient = new MongoClient(this.mongoUrl);
	}

	async initialize() {
		let rabbitConnected = false
		let mongoConnected = false
		let attempts = 0
		let maxAttempts = 10
		while (attempts < maxAttempts) {
			if (!rabbitConnected) {
				try {
					// Connect to RabbitMQ
					this.connection = await amqp.connect('amqp://admin:admin@rabbitmq:5672');
					this.channel = await this.connection.createChannel();
					await this.channel.assertExchange(this.exchangeName, 'direct', { durable: true });
					await this.channel.assertQueue(this.queueName, { durable: true });
					await this.channel.bindQueue(this.queueName, this.exchangeName, this.queueName);
					console.log('Connected to RabbitMQ');
					rabbitConnected = true
				} catch (error) {
					attempts++
					console.error('Failed to initialize ConsumerService Rabbit ?:', error);
					await new Promise(resolve => setTimeout(resolve, 5000));
				}
			}
			if (!mongoConnected) {
				try {
					// Connect to MongoDB
					await this.mongoClient.connect();
					console.log('Connected to MongoDB');
					console.log(`ConsumerService connected to exchange: ${this.exchangeName}, queue: ${this.queueName}`);
					mongoConnected = true
				} catch (error) {
					attempts++
					console.error('Failed to initialize ConsumerService Mongo ?:', error);
					await new Promise(resolve => setTimeout(resolve, 5000));
				}
			}

			if (mongoConnected && rabbitConnected) {
				attempts = maxAttempts
			}
		}

		if (!this.connection) {
			console.error('Failed to connect to RabbitMQ after multiple attempts.');
			process.exit(1); // Exit with an error code if unable to connect
		}
	}

	async startConsuming() {
		if (!this.channel) {
			throw new Error('Channel is not initialized.');
		}

		// Set up a consumer for the queue
		this.channel.consume(this.queueName, async (msg: Message | null) => {
			if (msg) {
				const streetsDataByCity = msg.content.toString();

				// Insert the message into MongoDB
				await this.saveMessageToMongo(streetsDataByCity);

				// Acknowledge that the message has been processed
				this.channel?.ack(msg);
			}
		});

		console.log(`ConsumerService is now listening for messages on ${this.queueName}`);
	}

	async saveMessageToMongo(message: string) {
		const parsedMessage: ParsedMessage = JSON.parse(message);
		const { city } = parsedMessage
		const { streets }: { streets: Street[] } = parsedMessage

		if (!city || !streets) {
			console.error('Issues with parsing data from rabbitmq');
			throw new Error('Issues with parsing data from rabbitmq');
		}

		const db = this.mongoClient.db(this.dbName);
		const collection: Collection<Document> = db.collection(city);
		try {
			await this.updateStreets(collection, streets)
			console.log('Streets saved for city:', city);
		} catch (error) {
			console.log('Failed Streets saved for city:', city);
		}

	}

	async updateStreets(collection: Collection<Document>, streets: Street[]) {
		for (const street of streets) {
			await collection.updateOne(
				{ id: street.streetId },
				{ $set: { name: street.name } },
				{ upsert: true }
			);
		}
	}

	async close() {
		await this.channel?.close();
		await this.connection?.close();
		await this.mongoClient.close();
		console.log('ConsumerService connection closed.');
	}
}

// Exported function for CLI
export async function startConsumer(exchange = 'israeli_streets_exchange', queue = 'israeli_streets_queue') {
	const consumer = new ConsumerService(exchange, queue);
	await consumer.initialize();
	await consumer.startConsuming();
}