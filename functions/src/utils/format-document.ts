import {Timestamp} from 'firebase-admin/firestore';
import {get, has} from 'json-pointer';
import {RuntimeCollectionConfig} from '../types/sync-config.interface';
import {safeFloat} from './safe-float';

/**
 * Format a Firestore document for BigQuery insertion
 * @param data The document data
 * @param documentId The document ID
 * @param config The collection configuration with parsed field definitions
 */
export async function formatDocument(
	data: any, 
	documentId: string,
	config: RuntimeCollectionConfig
): Promise<any> {
	let document: any = {
		documentId
	};

	if (config.includeParentIdInDocumentId && data.parentId) {
		document.documentId = `${data.parentId}-${document.documentId}`;
	}

	if (config.transformUrl) {
		const res = await fetch(config.transformUrl, {
			method: 'POST',
			headers: {
				'Content-Type': 'application/json'
			},
			body: JSON.stringify({documentId: document.documentId, ...data})
		});

		data = await res.json();
	}

	const trackedKeys = config.fields;

	trackedKeys.forEach(track => {
		let value = track.accessor(data);

		switch (track.type) {
			case 'NUMERIC':
			case 'FLOAT64':
			case 'INT64':
				if (track.method) {
					value = track.method(value);
				}

				if (typeof value === 'string') {
					value = parseFloat(value);
				}

				if (!value && value !== 0) {
					value = null;
				}

				if (typeof value !== 'number') {
					value = null;
				}

				value = safeFloat(value);

				break;
			case 'BIGNUMERIC':
			case 'BIGDECIMAL':
				if (track.method) {
					value = track.method(value);
				}

				if (typeof value === 'string') {
					value = parseFloat(value);
				}

				if (!value && value !== 0) {
					value = null;
				}

				if (typeof value !== 'number') {
					value = null;
				}

				break;
			case 'ARRAY':

				if (!Array.isArray(value)) {
					if (!value && value !== false && value !== 0) {
						value = [];
					} else {
						value = [value];
					}
				}

				if (track.formater) {
					value = [].concat(...value.map((v: any) => {

						let val: any = v;

						if (typeof v === 'object') {
							if (track.formater && has(v, track.formater!)) {
								val = get(v, track.formater!);
							}
						}

						return track.method ? track.method(val) : val;
					}));
				}
				value = (track.method ? value.map(track.method) : value).filter(Boolean).flat();

				break;
			case 'TIMESTAMP':
			case 'DATETIME':
				if (track.method) {
					value = track.method(value);
				}

				if (!value) {
					value = null;
					break;
				}

				/**
				 * We assume the value is a miliseconds timestamp
				 */
				try {
					if (typeof value === 'number' || typeof value === 'string') {
						value = new Date(value).toISOString();
					} else if (value instanceof Date) {
						value = value.toISOString();
					} else if (value instanceof Timestamp) {
						value = value.toDate().toISOString();
					} else {
						value = null;
					}
				} catch (e) {
					value = null;
				}


				break;
			case 'DATE':
				if (track.method) {
					value = track.method(value);
				}
				
				if (!value) {
					value = null;
					break;
				}

				/**
				 * We assume the value is a miliseconds timestamp
				 */
				try {
					if (typeof value === 'number' || typeof value === 'string') {
						value = new Date(value).toISOString().split('T')[0];
					} else if (value instanceof Date) {
						value = value.toISOString().split('T')[0];
					} else if (value instanceof Timestamp) {
						value = value.toDate().toISOString().split('T')[0];
					} else {
						value = null;
					}
				} catch (e) {
					value = null;
				}

				break;
			case 'BOOL':
				if (track.method) {
					value = track.method(value);
				}

				value = Boolean(value);

				if (typeof value !== 'boolean') {
					value = null;
				}

				break;
			case 'STRING':
				if (track.method) {
					value = track.method(value);
				}

				if (typeof value !== 'string') {
					value = null;
				}

				break;
			case 'JSON':
				if (track.method) {
					value = track.method(value);
				} else {
					value = JSON.stringify(value);
				}

				if (
					typeof value === 'string' ||
					typeof value === 'boolean' ||
					typeof value === 'number'
				) {
					value = null;
				}

				break;
			case 'REPEATED':
				if (
					typeof value === 'string' ||
					typeof value === 'boolean' ||
					typeof value === 'number'
				) {
					value = null;
				}

				break;
		}

		document[track.key] = value;
	});

	return document;
}