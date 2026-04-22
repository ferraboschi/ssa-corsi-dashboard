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

// ---- Segmentation ----

// Build a coarse segment key for grouping similar courses. Uses:
//   level   — 'certificato' | 'introduttivo' | 'shochu' | 'masterclass' | 'other'
//   online  — boolean
//   educator — raw educator name string, lowercased, or '' if unknown
// City is intentionally NOT in the key because we have too few courses per city;
// city is used for TIE-BREAK only when ranking similarity later.
function segmentKey(course) {
  const h = (course.handle || '').toLowerCase();
  let level = 'other';
  if (h.includes('certificato')) level = 'certificato';
  else if (h.includes('introduttivo')) level = 'introduttivo';
  else if (h.includes('shochu')) level = 'shochu';
  else if (h.includes('masterclass')) level = 'masterclass';
  const online = isOnlineCourse(course);
  const edu = (course.educatorName || '').toLowerCase().trim();
  return [level, online ? 'online' : 'presenza', edu].join('|');
}

// Given a list of past courses (all with known outcome) and a target
// "daysSinceOpening" value, return:
//   { sampleSize, medianAtPoint, p25AtPoint, p75AtPoint,
//     finalMedian, happenedRate }
//
// "enrolledAtPoint" is approximated for past courses: since we don't have a
// time-series of enrolment, we use the FINAL enrolment scaled by the ratio of
// the target daysSinceOpening to the course's total enrolment window. This is
// a coarse approximation that is OK for our use: a linear growth model.
// Callers must treat the returned median as a rough comparator, not ground truth.
function historicalBaselineAtDays(pastCourses, targetDaysSinceOpening) {
  const enrolledAtPoint = [];
  const enrolledFinal = [];
  let happenedCount = 0;

  for (const pc of pastCourses) {
    const opening = toDate(pc.created_at) || toDate(pc.published_at);
    const courseDate = toDate(pc.eventDate); // if we have it; else fall back
    const totalWindow = (opening && courseDate)
      ? Math.max(1, daysBetween(opening, courseDate))
      : null;
    const finalEnrolled = (pc.students || []).length || pc.enrollmentCount || 0;
    enrolledFinal.push(finalEnrolled);
    if (classifyOutcome(pc) === 'happened') happenedCount++;

    if (totalWindow && targetDaysSinceOpening <= totalWindow) {
      // Linear approximation: at targetDays of totalWindow, enrolment ~ final * target/total
      enrolledAtPoint.push(finalEnrolled * (targetDaysSinceOpening / totalWindow));
    } else {
      enrolledAtPoint.push(finalEnrolled);
    }
  }

  const median = (arr) => {
    if (arr.length === 0) return null;
    const s = arr.slice().sort((a, b) => a - b);
    const mid = Math.floor(s.length / 2);
    return s.length % 2 === 0 ? (s[mid - 1] + s[mid]) / 2 : s[mid];
  };
  const percentile = (arr, p) => {
    if (arr.length === 0) return null;
    const s = arr.slice().sort((a, b) => a - b);
    const idx = Math.min(s.length - 1, Math.floor(p * s.length));
    return s[idx];
  };

  return {
    sampleSize: pastCourses.length,
    medianAtPoint: median(enrolledAtPoint),
    p25AtPoint: percentile(enrolledAtPoint, 0.25),
    p75AtPoint: percentile(enrolledAtPoint, 0.75),
    finalMedian: median(enrolledFinal),
    happenedRate: pastCourses.length ? happenedCount / pastCourses.length : null,
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
  segmentKey,
  historicalBaselineAtDays,
};
