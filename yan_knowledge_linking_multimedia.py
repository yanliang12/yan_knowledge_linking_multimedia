##############yan_knowledge_linking_multimedia.py##############
import re 
import numpy 
import itertools

import yan_dbpedia_query

from yan_ocr import extract_text
from yan_tts import speech_to_text
from yan_sentence_segmentation import text_to_sentences
from yan_entity_linking import entity_linking

from yan_neo4j import start_neo4j
from yan_neo4j import create_neo4j_session
from yan_neo4j import ingest_knowledge_triplets_to_neo4j

import hashlib
str_md5 = lambda x: hashlib.md5(x.encode('utf-16')).hexdigest()

start_neo4j(
	http_port = "6794", 
	bolt_port = "6711",
	neo4j_path = '/home/yan/')

neo4j_session = create_neo4j_session(
	bolt_port = "6711")

##############

global query_wikipage_ids_all
global query_wikipage_ids_triplets_all

def document_to_sentences(
	document_path,
	):
	text = None
	if bool(re.search(r'\.(wav|mp3)$', document_path.lower())):
		text = speech_to_text(document_path)
	if bool(re.search(r'\.(jpg|png|jpeg)$', document_path.lower())):
		text = extract_text(document_path)
		text = [t['text'] for t in text]
		text = '\n'.join(text)
	if bool(re.search(r'\.(txt)$', document_path.lower())):
		f = open(document_path, "r") 
		text =f.read()
	if text is not None and len(text) > 1:
		sentences = text_to_sentences(text)
	return sentences

def knowledge_linking_from_mentioned_entities(
	query_wikipage_ids,
	triplets,
	):
	top_subject_object_triplets = yan_dbpedia_query.find_top_subject_object_for_each_entity(
		query_wikipage_ids,
		triplets,
		top_triplet_number = 3,
		)
	top_between_relation_tripltes = yan_dbpedia_query.find_top_relations_between_entity_pairs(
		query_wikipage_ids,
		triplets,
		top_triplet_number = 2,
		)
	top_common_subject_object_tripltes = yan_dbpedia_query.find_top_common_subject_object_of_entity_pairs(
		query_wikipage_ids,
		triplets,
		top_triplet_number = 2,
		)
	neo4j_triplets = top_common_subject_object_tripltes + top_subject_object_triplets + top_between_relation_tripltes
	ingest_knowledge_triplets_to_neo4j(
		neo4j_triplets, 
		neo4j_session,
		delete_data = False)

def process_a_new_documnet(
	document_path,
	):
	global query_wikipage_ids_all
	global query_wikipage_ids_triplets_all
	document_id = str_md5(document_path)
	sentences = document_to_sentences(
		document_path = document_path,
		)
	###build the document and sentence link
	triplets = []
	for s in sentences:
		sentence_id = str_md5(s)
		triplets.append({
			'subject':document_id,
			'subject_type':"document",
			'subject_name':document_path,
			'object':sentence_id,
			'object_type':"sentence",
			'object_name':s,
			'relation':"contain",
		})
	ingest_knowledge_triplets_to_neo4j(
		triplets, 
		neo4j_session,
		delete_data = False)
	####build the sentence entity link
	query_wikipage_ids_sentence = []
	query_wikipage_ids_triplets_sentence = []
	for s in sentences:
		sentence_id = str_md5(s)
		mentions = entity_linking(s)
		query_wikipage_ids = [e['entity_wikipage_id'] for e in mentions]
		query_wikipage_ids_triplets = yan_dbpedia_query.find_triplets_of_entities(
			query_wikipage_ids,
			)
		entity_name_type = yan_dbpedia_query.find_entity_id_and_type(
			query_wikipage_ids,
			query_wikipage_ids_triplets,
			)
		triplets = []
		for e in entity_name_type:
			triplets.append({
				'subject':sentence_id,
				'subject_type':"sentence",
				'subject_name':s,
				'object':e['entity_id'],
				'object_type':e['entity_type'],
				'object_name':e['entity_name'],
				'relation':"mention",
			})
		ingest_knowledge_triplets_to_neo4j(
			triplets, 
			neo4j_session,
			delete_data = False)
		###add this sentence results to the global
		query_wikipage_ids_all += query_wikipage_ids
		query_wikipage_ids_triplets_all += query_wikipage_ids_triplets
	#########update the network of related entities
	query_wikipage_ids_all = list(set(query_wikipage_ids_all))
	query_wikipage_ids_triplets_all = [dict(t) 
		for t in {tuple(d.items()) 
		for d in query_wikipage_ids_triplets_all}]
	####
	knowledge_linking_from_mentioned_entities(
		query_wikipage_ids_all,
		query_wikipage_ids_triplets_all,
		)

def initilize_result():
	query_wikipage_ids_all = []
	query_wikipage_ids_triplets_all = []
	ingest_knowledge_triplets_to_neo4j(
		triplets = [], 
		neo4j_session = neo4j_session,
		delete_data = True,
		)

##############yan_knowledge_linking_multimedia.py##############