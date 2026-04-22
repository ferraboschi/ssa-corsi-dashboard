// recommendations.js
// Pure functions. No I/O, no side effects. All inputs passed in.
// Consumed by server.js to compute per-course recommendations from the
// already-cached Shopify course list.

'use strict';

const MIN_STUDENTS_PRESENZA = 6;
const MIN_STUDENTS_ONLINE = 3;

function isOnlineCourse(course) {
  const h = (course.handle || '').toLowerCase();
  const tags = (course.tags || '').toString().toLowerCase();
  return h.includes('online') || tags.includes('online');
}

function minStudentsFor(course) {
  return isOnlineCourse(course) ? MIN_STUDENTS_ONLINE : MIN_STUDENTS_PRESENZA;
}

// Classify a past course by its final enrolment.
//   happened  -> met or exceeded the minimum
//   cancelled -> did not meet the minimum (so we assume it was cancelled
//                or merged into another date; we don't have a ground-truth
//                field for this, so enrolment is our best proxy)
//   unknown   -> we cannot classify (no enrolment data)
function classifyOutcome(course) {
  const enrolled = (course.students || []).length || course.enrollmentCount || 0;
  if (typeof enrolled !== 'number') return 'unknown';
  const min = minStudentsFor(course);
  return enrolled >= min ? 'happened' : 'cancelled';
}

module.exports = {
  MIN_STUDENTS_PRESENZA,
  MIN_STUDENTS_ONLINE,
  isOnlineCourse,
  minStudentsFor,
  classifyOutcome,
};
