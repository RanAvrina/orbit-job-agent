import test from 'node:test';
import assert from 'node:assert/strict';
import {evaluate, authorized} from '../server.mjs';

test('junior ERP role in Beer Sheva is eligible', () => {
 const result=evaluate({title:'Junior ERP Implementer',location:'Beer Sheva, Israel',description:'SQL and Priority'});
 assert.equal(result.eligible,true);
 assert.ok(result.score>=65);
});

test('senior positions are rejected', () => {
 const result=evaluate({title:'Senior ERP Implementer',location:'Tel Aviv, Israel',description:'SQL'});
 assert.equal(result.eligible,false);
 assert.equal(result.reason,'תפקיד בכיר');
});

test('unrelated job is rejected', () => {
 const result=evaluate({title:'Restaurant Chef',location:'Tel Aviv, Israel',description:'Kitchen'});
 assert.equal(result.eligible,false);
});

test('foreign location is rejected', () => {
 const result=evaluate({title:'Junior Data Analyst',location:'London, UK',description:'SQL'});
 assert.equal(result.eligible,false);
});

test('missing bearer token cannot authenticate', () => {
 assert.equal(authorized({headers:{}}),false);
});
