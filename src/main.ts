import { startPublisher } from './publisherService/publisherService';
import { startConsumer } from './consumerService/consumerService';

const args = process.argv.slice(2);

async function run() {
    const command = args[0];

    try {
        switch (command) {
            case 'publish':
                const cityName = args[1];
                if (!cityName) {
                    console.log('Usage: publish <cityName>');
                    process.exit(1);
                }

                await startPublisher(cityName)

                break;

            case 'consume':
                console.log('Starting consumer...');
                await startConsumer();
                break;

            default:
                console.log(`Unknown command: ${command}`);
                console.log('Usage: node main.js <publish|consume> [message]');
        }
    } catch (error) {
        console.error('Error:', error);
    }
}

run();