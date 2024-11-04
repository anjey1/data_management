import * as amqp from 'amqplib';
import { Channel, Connection } from 'amqplib';
import { StreetsService } from '../israeliStreets';

export class PublisherService {
	private connection: Connection | null = null;
	private channel: Channel | null = null;
	private readonly exchangeName: string;
	private readonly queueName: string;

	constructor(exchangeName: string, queueName: string) {
		this.exchangeName = exchangeName;
		this.queueName = queueName;
	}

	async initialize() {
		try {
			this.connection = await amqp.connect('amqp://admin:admin@localhost:5672');
			this.channel = await this.connection.createChannel();

			await this.channel.assertExchange(this.exchangeName, 'direct', { durable: true });
			await this.channel.assertQueue(this.queueName, { durable: true });
			await this.channel.bindQueue(this.queueName, this.exchangeName, this.queueName);

			console.log(`Connected to exchange: ${this.exchangeName}, queue: ${this.queueName}`);
		} catch (error) {
			console.error('Failed to initialize PublisherService:', error);
			throw error;
		}
	}

	async publishStreets(cityName) {
		if (!this.channel) {
			throw new Error('Channel is not initialized.');
		}

		let streetData = {}

		try {
			streetData = await StreetsService.getStreetsInCity(cityName)
			const messageBuffer = Buffer.from(JSON.stringify(streetData));
			const success = this.channel.publish(
				this.exchangeName,
				this.queueName,
				messageBuffer);

			if (success) {
				console.log(`Message published to ${this.queueName}:`, cityName);
			} else {
				console.error('Failed to publish message');
			}

		} catch (error) {
			console.log("Error fetching streets by city:", cityName);
		}
	}

	async close() {
		await this.channel?.close();
		await this.connection?.close();
		console.log('PublisherService connection closed.');
	}
}

// Exported function for CLI
export async function startPublisher(cityName: string, exchange = 'israeli_streets_exchange', queue = 'israeli_streets_queue') {
	const publisher = new PublisherService(exchange, queue);
	await publisher.initialize();
	await publisher.publishStreets(cityName);
	await publisher.close();
}