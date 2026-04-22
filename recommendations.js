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

// ---- Date helpers ----

// Parse Shopify created_at / published_at into a JS Date, tolerating nulls.
function toDate(value) {
  if (!value) return null;
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? null : d;
}

// Day difference (b - a), rounded down; negative if b is before a.
function daysBetween(a, b) {
  if (!a || !b) return null;
  const ms = b.getTime() - a.getTime();
  return Math.floor(ms / (1000 * 60 * 60 * 24));
}

// ---- Per-course metrics ----

// Compute raw metrics for a FUTURE course. Caller must pass `courseDate` (a
// Date for when the course runs) and `now` (reference time, usually Date.now).
// `openingDate` is when enrolment became possible — we use Shopify created_at
// as a proxy.
function courseMetrics(course, courseDate, now) {
  const enrolled = (course.students || []).length || course.enrollmentCount || 0;
  const opening = toDate(course.created_at) || toDate(course.published_at);
  const daysSinceOpening = opening ? Math.max(1, daysBetween(opening, now)) : null;
  const daysUntilCourse = courseDate ? daysBetween(now, courseDate) : null;
  const velocity = daysSinceOpening ? enrolled / daysSinceOpening : null;
  const projection = (velocity != null && daysUntilCourse != null && daysUntilCourse > 0)
    ? Math.floor(enrolled + velocity * daysUntilCourse)
    : enrolled;
  return {
    enrolled,
    min: minStudentsFor(course),
    daysSinceOpening,
    daysUntilCourse,
    velocity,
    projection,
  };
}

module.exports = {
  MIN_STUDENTS_PRESENZA,
  MIN_STUDENTS_ONLINE,
  isOnlineCourse,
  minStudentsFor,
  classifyOutcome,
  toDate,
  daysBetween,
  courseMetrics,
};
