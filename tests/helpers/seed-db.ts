import type { TestDB } from './db'
import { faker } from '@faker-js/faker'
import {
  generateOrganizations,
  generateUsers,
  generateProjects,
  generateMilestones,
  generateLabels,
  generateTasks,
  generateComments,
  generateReactions,
  generateAttachments,
  generateActivities,
  generateNotifications,
} from '../fixtures/seed'

faker.seed(12345)

export interface SeedResult {
  organizationIds: number[]
  userIds: number[]
  projectIds: number[]
  milestoneIds: number[]
  labelIds: number[]
  taskIds: number[]
  commentIds: number[]
}

async function safeDeleteMany(fn: () => Promise<unknown>): Promise<void> {
  try {
    await fn()
  } catch (e: unknown) {
    const error = e as Error
    if (!error.message?.includes('does not exist')) {
      throw e
    }
  }
}

export async function seedDatabase(db: TestDB): Promise<SeedResult> {
  const existingUsers = await db.prisma.user.count()

  if (existingUsers > 0) {
    console.log('Database already seeded, skipping...')

    const users = await db.prisma.user.findMany({ select: { id: true } })
    const organizations = await db.prisma.organization.findMany({
      select: { id: true },
    })
    const projects = await db.prisma.project.findMany({ select: { id: true } })
    const milestones = await db.prisma.milestone.findMany({
      select: { id: true },
    })
    const labels = await db.prisma.label.findMany({ select: { id: true } })
    const tasks = await db.prisma.task.findMany({ select: { id: true } })
    const comments = await db.prisma.comment.findMany({ select: { id: true } })

    return {
      organizationIds: organizations.map((o: any) => o.id),
      userIds: users.map((u: any) => u.id),
      projectIds: projects.map((p: any) => p.id),
      milestoneIds: milestones.map((m: any) => m.id),
      labelIds: labels.map((l: any) => l.id),
      taskIds: tasks.map((t: any) => t.id),
      commentIds: comments.map((c: any) => c.id),
    }
  }

  console.log('Seeding database...')
  const start = performance.now()

  await safeDeleteMany(() => db.prisma.reaction.deleteMany())
  await safeDeleteMany(() => db.prisma.comment.deleteMany())
  await safeDeleteMany(() => db.prisma.attachment.deleteMany())
  await safeDeleteMany(() => db.prisma.activity.deleteMany())
  await safeDeleteMany(() => db.prisma.notification.deleteMany())
  await safeDeleteMany(() => db.prisma.taskLabel.deleteMany())
  await safeDeleteMany(() => db.prisma.task.deleteMany())
  await safeDeleteMany(() => db.prisma.label.deleteMany())
  await safeDeleteMany(() => db.prisma.milestone.deleteMany())
  await safeDeleteMany(() => db.prisma.project.deleteMany())
  await safeDeleteMany(() => db.prisma.invitation.deleteMany())
  await safeDeleteMany(() => db.prisma.member.deleteMany())
  await safeDeleteMany(() => db.prisma.user.deleteMany())
  await safeDeleteMany(() => db.prisma.organization.deleteMany())

  const organizationIds: number[] = []
  const userIds: number[] = []
  const projectIds: number[] = []
  const milestoneIds: number[] = []
  const labelIds: number[] = []
  const taskIds: number[] = []
  const commentIds: number[] = []

  const orgs = generateOrganizations()
  for (const orgData of orgs) {
    const org = await db.prisma.organization.create({ data: orgData })
    organizationIds.push(org.id)
  }

  const totalUsers = organizationIds.length * 5
  const users = generateUsers(totalUsers)
  for (const userData of users) {
    try {
      const user = await db.prisma.user.create({ data: userData })
      userIds.push(user.id)
    } catch {
      const user = await db.prisma.user.create({
        data: {
          ...userData,
          email: `${faker.string.alphanumeric(8)}@test.com`,
        },
      })
      userIds.push(user.id)
    }
  }

  for (let i = 0; i < organizationIds.length; i++) {
    const orgId = organizationIds[i]
    const orgUserIds = userIds.slice(i * 5, (i + 1) * 5)

    for (let j = 0; j < orgUserIds.length; j++) {
      await db.prisma.member.create({
        data: {
          organizationId: orgId,
          userId: orgUserIds[j],
          role:
            j === 0
              ? 'OWNER'
              : faker.helpers.arrayElement(['ADMIN', 'MEMBER', 'VIEWER']),
        },
      })
    }

    const projects = generateProjects()
    for (const projectData of projects) {
      const project = await db.prisma.project.create({
        data: {
          ...projectData,
          organization: { connect: { id: orgId } },
        },
      })
      projectIds.push(project.id)

      const milestones = generateMilestones()
      for (const msData of milestones) {
        const milestone = await db.prisma.milestone.create({
          data: {
            ...msData,
            project: { connect: { id: project.id } },
          },
        })
        milestoneIds.push(milestone.id)
      }

      const projectMilestoneIds = milestoneIds.slice(-milestones.length)

      const labels = generateLabels()
      for (const labelData of labels) {
        try {
          const label = await db.prisma.label.create({
            data: {
              ...labelData,
              project: { connect: { id: project.id } },
            },
          })
          labelIds.push(label.id)
        } catch {}
      }

      const projectLabelIds = labelIds.slice(-labels.length)

      const tasks = generateTasks()
      for (const taskData of tasks) {
        const creatorId = faker.helpers.arrayElement(orgUserIds)
        const assigneeId = faker.datatype.boolean(0.7)
          ? faker.helpers.arrayElement(orgUserIds)
          : null
        const milestoneId =
          faker.datatype.boolean(0.5) && projectMilestoneIds.length > 0
            ? faker.helpers.arrayElement(projectMilestoneIds)
            : null

        const task = await db.prisma.task.create({
          data: {
            ...taskData,
            project: { connect: { id: project.id } },
            creator: { connect: { id: creatorId } },
            assignee: assigneeId ? { connect: { id: assigneeId } } : undefined,
            milestone: milestoneId
              ? { connect: { id: milestoneId } }
              : undefined,
          },
        })
        taskIds.push(task.id)

        if (projectLabelIds.length > 0 && faker.datatype.boolean(0.6)) {
          const taskLabelIds = faker.helpers.arrayElements(projectLabelIds, {
            min: 1,
            max: 2,
          })
          for (const labelId of taskLabelIds) {
            try {
              await db.prisma.taskLabel.create({
                data: { taskId: task.id, labelId },
              })
            } catch {}
          }
        }

        const subtaskCount = faker.number.int({ min: 0, max: 2 })
        for (let s = 0; s < subtaskCount; s++) {
          const subtask = await db.prisma.task.create({
            data: {
              title: faker.hacker.phrase(),
              description: faker.datatype.boolean(0.5)
                ? faker.lorem.sentence()
                : null,
              status: faker.helpers.arrayElement([
                'TODO',
                'IN_PROGRESS',
                'DONE',
              ]),
              priority: 'MEDIUM',
              position: s,
              project: { connect: { id: project.id } },
              creator: { connect: { id: creatorId } },
              parent: { connect: { id: task.id } },
            },
          })
          taskIds.push(subtask.id)
        }

        const comments = generateComments()
        for (const commentData of comments) {
          const authorId = faker.helpers.arrayElement(orgUserIds)
          const comment = await db.prisma.comment.create({
            data: {
              ...commentData,
              task: { connect: { id: task.id } },
              author: { connect: { id: authorId } },
            },
          })
          commentIds.push(comment.id)

          const reactions = generateReactions()
          for (const reactionData of reactions) {
            await db.prisma.reaction.create({
              data: {
                ...reactionData,
                comment: { connect: { id: comment.id } },
              },
            })
          }

          const replyCount = faker.number.int({ min: 0, max: 1 })
          for (let r = 0; r < replyCount; r++) {
            const replyAuthorId = faker.helpers.arrayElement(orgUserIds)
            const reply = await db.prisma.comment.create({
              data: {
                content: faker.lorem.sentence(),
                isEdited: faker.datatype.boolean(0.05),
                task: { connect: { id: task.id } },
                author: { connect: { id: replyAuthorId } },
                parent: { connect: { id: comment.id } },
              },
            })
            commentIds.push(reply.id)
          }
        }

        const attachments = generateAttachments()
        for (const attachmentData of attachments) {
          await db.prisma.attachment.create({
            data: {
              ...attachmentData,
              task: { connect: { id: task.id } },
            },
          })
        }

        const activities = generateActivities()
        for (const activityData of activities) {
          const actorId = faker.helpers.arrayElement(orgUserIds)
          await db.prisma.activity.create({
            data: {
              ...activityData,
              task: { connect: { id: task.id } },
              user: { connect: { id: actorId } },
            },
          })
        }
      }
    }
  }

  for (const userId of userIds) {
    const notifications = generateNotifications()
    for (const notificationData of notifications) {
      await db.prisma.notification.create({
        data: {
          ...notificationData,
          user: { connect: { id: userId } },
        },
      })
    }
  }

  const end = performance.now()
  console.log(`Seeding completed in ${((end - start) / 1000).toFixed(2)}s`)
  console.log(`  Organizations: ${organizationIds.length}`)
  console.log(`  Users: ${userIds.length}`)
  console.log(`  Projects: ${projectIds.length}`)
  console.log(`  Milestones: ${milestoneIds.length}`)
  console.log(`  Labels: ${labelIds.length}`)
  console.log(`  Tasks: ${taskIds.length}`)
  console.log(`  Comments: ${commentIds.length}`)

  return {
    organizationIds,
    userIds,
    projectIds,
    milestoneIds,
    labelIds,
    taskIds,
    commentIds,
  }
}
