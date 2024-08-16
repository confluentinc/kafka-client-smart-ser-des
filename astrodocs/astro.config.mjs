import { defineConfig } from 'astro/config'
import starlight from '@astrojs/starlight'

import robotsTxt from 'astro-robots-txt'

// https://astro.build/config
export default defineConfig({
	build: {
		inlineStylesheets: 'always'
	},
	integrations: [starlight({
		lastUpdated: true,
		title: 'Kafka Client Smart Ser Des',
		editLink: {
			baseUrl: 'https://github.com/confluentinc/kafka-client-smart-ser-des/edit/main/astrodocs/'
		},
		logo: {
			light: '/src/assets/confluent.svg',
			dark: '/src/assets/confluent-dark.svg',
		},
		sidebar: [{
			label: 'Home',
			items: [{
				label: 'Introduction',
				link: '/'
			}]
		}, {
		    label: 'Connect',
		    autogenerate: {
		        directory: 'connect'
		    }
		}, {
			label: 'Serializer',
			autogenerate: {
				directory: 'serializer'
			}
		}, {
			label: 'Deserializer',
			autogenerate: {
				directory: 'deserializer'
			}
		}]
	}),
		robotsTxt({
        			policy: [{
        				userAgent: '*',
        				disallow: ['/']
        			}]
        		})
        	],
        	site: 'https://confluentinc.github.io/kafka-client-smart-ser-des/',
        	base: '/'
        })
