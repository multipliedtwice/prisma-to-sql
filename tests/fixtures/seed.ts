import { faker } from '@faker-js/faker'

faker.seed(12345)

const COUNTS = {
  organizations: 10,
  usersPerOrg: 10,
  projectsPerOrg: 10,
  milestonesPerProject: 2,
  labelsPerProject: 4,
  tasksPerProject: 20,
  subtasksPerTask: 0,
  commentsPerTask: 1,
  repliesPerComment: 0,
  reactionsPerComment: 1,
  attachmentsPerTask: 0,
  activitiesPerTask: 1,
  notificationsPerUser: 1,
}

export function generateOrganizations() {
  return Array.from({ length: COUNTS.organizations }, (_, i) => ({
    name: faker.company.name(),
    slug: faker.helpers.slugify(faker.company.name()).toLowerCase() + `-${i}`,
    plan: faker.helpers.arrayElement(['FREE', 'STARTER', 'PRO', 'ENTERPRISE']),
    settings: faker.datatype.boolean()
      ? JSON.stringify({
          features: faker.helpers.arrayElements(['analytics', 'api', 'sso'], {
            min: 1,
            max: 3,
          }),
          limits: { members: faker.number.int({ min: 5, max: 100 }) },
        })
      : undefined,
  }))
}

export function generateUsers(count: number) {
  return Array.from({ length: count }, () => ({
    email: faker.internet.email().toLowerCase(),
    name: faker.datatype.boolean(0.9) ? faker.person.fullName() : undefined,
    avatarUrl: faker.datatype.boolean(0.7) ? faker.image.avatar() : undefined,
    role: faker.helpers.arrayElement(['USER', 'ADMIN', 'SUPERADMIN']),
    status: faker.helpers.weightedArrayElement([
      { value: 'ACTIVE', weight: 80 },
      { value: 'INACTIVE', weight: 10 },
      { value: 'SUSPENDED', weight: 5 },
      { value: 'DELETED', weight: 5 },
    ]),
    metadata: faker.datatype.boolean(0.6)
      ? JSON.stringify({
          timezone: faker.location.timeZone(),
          theme: faker.helpers.arrayElement(['light', 'dark', 'system']),
        })
      : undefined,
    tags: JSON.stringify(
      faker.helpers.arrayElements(
        ['frontend', 'backend', 'devops', 'design', 'pm'],
        { min: 0, max: 3 },
      ),
    ),
    lastLoginAt: faker.datatype.boolean(0.8)
      ? faker.date.recent({ days: 30 })
      : undefined,
  }))
}

export function generateProjects() {
  return Array.from({ length: COUNTS.projectsPerOrg }, () => {
    const startDate = faker.datatype.boolean(0.7)
      ? faker.date.past({ years: 1 })
      : undefined

    return {
      name: faker.commerce.productName(),
      description: faker.datatype.boolean(0.8)
        ? faker.lorem.paragraph()
        : undefined,
      color: faker.color.rgb(),
      status: faker.helpers.weightedArrayElement([
        { value: 'ACTIVE', weight: 60 },
        { value: 'COMPLETED', weight: 20 },
        { value: 'ARCHIVED', weight: 10 },
        { value: 'ON_HOLD', weight: 10 },
      ]),
      isPublic: faker.datatype.boolean(0.2),
      budget: faker.datatype.boolean(0.5)
        ? faker.number.float({ min: 1000, max: 100000, fractionDigits: 2 })
        : undefined,
      startDate,
      metadata: faker.datatype.boolean(0.4)
        ? JSON.stringify({
            priority: faker.helpers.arrayElement(['low', 'medium', 'high']),
          })
        : undefined,
    }
  })
}

export function generateMilestones() {
  return Array.from({ length: COUNTS.milestonesPerProject }, () => ({
    name: faker.helpers.arrayElement([
      'Phase 1',
      'Phase 2',
      'MVP',
      'Beta',
      'Launch',
    ]),
    description: faker.datatype.boolean(0.6)
      ? faker.lorem.sentence()
      : undefined,
    dueDate: faker.datatype.boolean(0.8)
      ? faker.date.future({ years: 1 })
      : undefined,
    completedAt: faker.datatype.boolean(0.3)
      ? faker.date.past({ years: 1 })
      : undefined,
  }))
}

export function generateLabels() {
  const labelNames = [
    'bug',
    'feature',
    'enhancement',
    'documentation',
    'urgent',
  ]
  return faker.helpers
    .arrayElements(labelNames, { min: 2, max: COUNTS.labelsPerProject })
    .map((name) => ({
      name,
      color: faker.color.rgb(),
    }))
}

export function generateTasks() {
  return Array.from({ length: COUNTS.tasksPerProject }, (_, i) => {
    const status = faker.helpers.weightedArrayElement([
      { value: 'TODO', weight: 30 },
      { value: 'IN_PROGRESS', weight: 25 },
      { value: 'IN_REVIEW', weight: 15 },
      { value: 'DONE', weight: 25 },
      { value: 'CANCELLED', weight: 5 },
    ])

    return {
      title: faker.hacker.phrase(),
      description: faker.datatype.boolean(0.7)
        ? faker.lorem.paragraphs({ min: 1, max: 2 })
        : undefined,
      status,
      priority: faker.helpers.arrayElement(['LOW', 'MEDIUM', 'HIGH', 'URGENT']),
      position: i,
      dueDate: faker.datatype.boolean(0.6)
        ? faker.date.future({ years: 1 })
        : undefined,
      completedAt:
        status === 'DONE' ? faker.date.recent({ days: 30 }) : undefined,
      estimatedHours: faker.datatype.boolean(0.5)
        ? faker.number.float({ min: 0.5, max: 40, fractionDigits: 1 })
        : undefined,
      metadata: faker.datatype.boolean(0.3)
        ? JSON.stringify({ storyPoints: faker.number.int({ min: 1, max: 13 }) })
        : undefined,
    }
  })
}

export function generateComments() {
  return Array.from(
    { length: faker.number.int({ min: 0, max: COUNTS.commentsPerTask }) },
    () => ({
      content: faker.lorem.paragraphs({ min: 1, max: 2 }),
      isEdited: faker.datatype.boolean(0.1),
    }),
  )
}

export function generateReactions() {
  const emojis = ['👍', '👎', '❤️', '🎉', '😄', '🚀']
  return Array.from(
    { length: faker.number.int({ min: 0, max: COUNTS.reactionsPerComment }) },
    () => ({
      emoji: faker.helpers.arrayElement(emojis),
    }),
  )
}

export function generateAttachments() {
  return Array.from(
    { length: faker.number.int({ min: 0, max: COUNTS.attachmentsPerTask }) },
    () => ({
      filename: faker.system.fileName(),
      url: faker.internet.url(),
      mimeType: faker.system.mimeType(),
      size: faker.number.int({ min: 1000, max: 10000000 }),
    }),
  )
}

export function generateActivities() {
  return Array.from(
    { length: faker.number.int({ min: 1, max: COUNTS.activitiesPerTask }) },
    () => ({
      action: faker.helpers.arrayElement([
        'TASK_CREATED',
        'TASK_UPDATED',
        'TASK_COMPLETED',
        'TASK_ASSIGNED',
        'COMMENT_ADDED',
      ]),
      details: faker.datatype.boolean(0.6)
        ? JSON.stringify({
            field: faker.helpers.arrayElement([
              'status',
              'priority',
              'assignee',
            ]),
            oldValue: faker.word.sample(),
            newValue: faker.word.sample(),
          })
        : undefined,
    }),
  )
}

export function generateNotifications() {
  return Array.from(
    { length: faker.number.int({ min: 0, max: COUNTS.notificationsPerUser }) },
    () => ({
      type: faker.helpers.arrayElement([
        'mention',
        'assignment',
        'comment',
        'due_date',
      ]),
      title: faker.lorem.sentence({ min: 3, max: 8 }),
      message: faker.datatype.boolean(0.7) ? faker.lorem.sentence() : undefined,
      isRead: faker.datatype.boolean(0.4),
      data: faker.datatype.boolean(0.5)
        ? JSON.stringify({ taskId: faker.number.int({ min: 1, max: 100 }) })
        : undefined,
    }),
  )
}
